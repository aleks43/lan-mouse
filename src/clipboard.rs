use crate::{client::ClientManager, connect::LanMouseSender, emulation::ClipboardSender};
use futures::FutureExt;
use lan_mouse_proto::{ClipboardChunk, ProtoEvent, clipboard_chunks};
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{cell::Cell, rc::Rc, time::Duration};
use tokio::process::Command;
use tokio::task::{JoinHandle, spawn_local};

/// how often the local clipboard is polled for changes
const POLL_INTERVAL: Duration = Duration::from_millis(300);

/// clipboard text larger than this is never broadcast
const MAX_BROADCAST_SIZE: usize = 1024 * 1024;

/// a clipboard tool that does not terminate within this time is killed
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

/// Clipboard text that is passed as a command line argument must not exceed
/// this size. Linux limits a single argument to 128 KiB (`MAX_ARG_STRLEN`).
const MAX_ARG_TEXT_SIZE: usize = 100 * 1024;

/// number of chunks sent back to back before pausing briefly. The chunks are
/// sent as unacknowledged datagrams, pacing keeps socket buffers from overflowing.
const CHUNKS_PER_BURST: usize = 8;
const BURST_PAUSE: Duration = Duration::from_millis(1);

/// a command line invocation used to read or write the clipboard
#[derive(Clone, Debug)]
struct CliCommand {
    program: &'static str,
    args: &'static [&'static str],
    /// the clipboard text is appended as the final argument
    text_arg: bool,
    /// the clipboard text is piped to stdin
    text_stdin: bool,
}

impl CliCommand {
    fn get(program: &'static str, args: &'static [&'static str]) -> Self {
        Self {
            program,
            args,
            text_arg: false,
            text_stdin: false,
        }
    }

    fn set(program: &'static str, args: &'static [&'static str], text_arg: bool) -> Self {
        Self {
            program,
            args,
            text_arg,
            text_stdin: !text_arg,
        }
    }

    fn probe(&self) -> CliCommand {
        CliCommand {
            program: self.program,
            args: match self.program {
                // xclip uses the single-dash spelling; `--version` exits with
                // an error even though the executable is otherwise usable.
                "xclip" => &["-version"],
                // pbpaste has no version flag. Invoke its normal read command;
                // `detect_tool` treats a successful spawn as availability because
                // an empty/non-text pasteboard can make pbpaste exit nonzero.
                "pbpaste" => &[],
                _ => &["--version"],
            },
            text_arg: false,
            text_stdin: false,
        }
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(self.program);
        cmd.args(self.args);
        // GUI-launched macOS apps may have no UTF-8 locale. In that case
        // pbcopy interprets UTF-8 bytes using the user's legacy Mac encoding.
        #[cfg(target_os = "macos")]
        if matches!(self.program, "pbcopy" | "pbpaste") {
            cmd.env("LC_CTYPE", "en_US.UTF-8");
        }
        cmd.stdin(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        // never leave a hung tool behind when the future is dropped or times out
        cmd.kill_on_drop(true);
        cmd
    }

    /// run the command and capture its output
    async fn read(&self) -> std::io::Result<std::process::Output> {
        let mut cmd = self.command();
        cmd.stdout(std::process::Stdio::piped());
        with_timeout(cmd.output()).await
    }

    /// run the command to write `text` to the clipboard
    async fn write(&self, text: &str) -> std::io::Result<()> {
        let mut cmd = self.command();
        // Tools like `wl-copy` and `xclip` fork into the background to serve
        // the selection. Their children must not inherit a pipe we wait on.
        cmd.stdout(std::process::Stdio::null());
        if self.text_arg {
            // The text ends up in the process list and must not be mistaken
            // for an option, which can not be ruled out for these tools.
            if text.starts_with('-') || text.len() > MAX_ARG_TEXT_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "`{}` can not safely be passed the text as an argument",
                        self.program
                    ),
                ));
            }
            cmd.arg(text);
        } else if self.text_stdin {
            cmd.stdin(std::process::Stdio::piped());
        }
        with_timeout(async {
            use tokio::io::AsyncWriteExt;
            let mut child = cmd.spawn()?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(text.as_bytes()).await?;
                stdin.flush().await?;
                drop(stdin);
            }
            let status = child.wait().await?;
            if status.success() {
                Ok(())
            } else {
                Err(std::io::Error::other(format!(
                    "`{}` exited with {status}",
                    self.program
                )))
            }
        })
        .await
    }
}

async fn with_timeout<T>(
    fut: impl std::future::Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
    match tokio::time::timeout(COMMAND_TIMEOUT, fut).await {
        Ok(result) => result,
        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
    }
}

/// clipboard access implemented with the platform's command line tools.
///
/// macOS provides `pbcopy`/`pbpaste`. On Wayland clipboard access is
/// restricted by the compositor: `wl-clipboard` works on compositors that
/// implement (wlr|ext)-data-control, GNOME requires the ClipboardNext
/// extension (which provides `cb`). On X11 we use `xclip`/`xsel`.
#[derive(Clone, Debug)]
struct CliTool {
    name: &'static str,
    get: CliCommand,
    set: CliCommand,
}

fn tool_candidates() -> Vec<CliTool> {
    // unused on platforms without a supported clipboard tool
    #[allow(unused_mut)]
    let mut tools = Vec::new();
    #[cfg(target_os = "macos")]
    tools.push(CliTool {
        name: "pbcopy/pbpaste",
        get: CliCommand::get("pbpaste", &[]),
        set: CliCommand::set("pbcopy", &[], false),
    });
    #[cfg(target_os = "linux")]
    {
        let wayland = std::env::var_os("WAYLAND_DISPLAY").is_some();
        let gnome = std::env::var("XDG_CURRENT_DESKTOP")
            .is_ok_and(|d| d.split(':').any(|e| e.eq_ignore_ascii_case("GNOME")));
        let x11 = std::env::var_os("DISPLAY").is_some();

        let wl_clipboard = || CliTool {
            name: "wl-clipboard",
            get: // `-t text` restricts the read to textual types, without it
            // wl-paste outputs the raw bytes of e.g. an image
            CliCommand::get("wl-paste", &["-n", "-t", "text"]),
            set: CliCommand::set("wl-copy", &[], false),
        };
        // GNOME provides no data-control protocol. ClipboardNext
        // (shell extension) exposes the `cb` CLI which proxies
        // clipboard access through the extension.
        let clipboard_next = || CliTool {
            name: "clipboard-next",
            get: CliCommand::get("cb", &["-n"]),
            set: CliCommand::set("cb", &["copy", "-n"], true),
        };
        let xclip = || CliTool {
            name: "xclip",
            get: CliCommand::get("xclip", &["-selection", "clipboard", "-o"]),
            set: CliCommand::set("xclip", &["-selection", "clipboard"], false),
        };
        let xsel = || CliTool {
            name: "xsel",
            get: CliCommand::get("xsel", &["--clipboard", "--output"]),
            set: CliCommand::set("xsel", &["--clipboard", "--input"], false),
        };

        // on GNOME, wl-clipboard is present on many systems but cannot
        // access the clipboard - prefer the GNOME-specific / XWayland bridges
        if gnome {
            tools.push(clipboard_next());
            if x11 {
                tools.push(xclip());
                tools.push(xsel());
            }
        } else if wayland {
            tools.push(wl_clipboard());
            tools.push(clipboard_next());
            if x11 {
                tools.push(xclip());
                tools.push(xsel());
            }
        } else if x11 {
            tools.push(xclip());
            tools.push(xsel());
        }
    }
    tools
}

async fn detect_tool() -> Option<CliTool> {
    for tool in tool_candidates() {
        match tool.get.probe().read().await {
            // `pbpaste` exits nonzero when there is no textual clipboard data,
            // which is normal at startup. Successful process creation is enough
            // to establish that the macOS clipboard backend is installed.
            Ok(output) if output.status.success() || tool.get.program == "pbpaste" => {
                log::info!("clipboard sync enabled, backend: {}", tool.name);
                return Some(tool);
            }
            _ => log::debug!("clipboard backend `{}` not available", tool.name),
        }
    }
    None
}

/// Strip trailing line breaks. The various tools differ in how they handle
/// them, so this is only used to *compare* clipboard contents, the text
/// that is shared is left untouched.
fn normalize(text: &str) -> &str {
    text.trim_end_matches(['\n', '\r'])
}

/// Decides which clipboard contents observed by polling have to be shared.
#[derive(Default)]
struct ChangeTracker {
    /// normalized text last seen on the clipboard (`None` before the first poll)
    seen: Option<String>,
}

impl ChangeTracker {
    /// Process the result of a poll. `text` is `None` if the clipboard holds
    /// no (valid UTF-8) text. Returns the text if it has to be shared.
    ///
    /// - the contents present at startup are never shared, so launching
    ///   lan-mouse does not overwrite the clipboards of the peers.
    /// - an empty or non-text clipboard is never shared, so it can not wipe
    ///   the clipboards of the peers.
    fn poll(&mut self, text: Option<String>) -> Option<String> {
        let first = self.seen.is_none();
        let key = normalize(text.as_deref().unwrap_or_default());
        if self.seen.as_deref() == Some(key) {
            return None;
        }
        self.seen = Some(key.to_owned());
        if first {
            return None;
        }
        text.filter(|t| !t.is_empty())
    }

    /// text received from a peer was written to the clipboard. It is
    /// remembered so that it is not shared back, but only for as long as
    /// the clipboard keeps these contents.
    fn remote_written(&mut self, text: &str) {
        self.seen = Some(normalize(text).to_owned());
    }
}

pub(crate) enum ClipboardEvent {
    /// the local clipboard changed, text should be shared with peers
    Changed(String),
    /// clipboard access is unavailable on this platform, syncing stopped
    Unavailable,
}

pub(crate) enum ClipboardRequest {
    /// apply text received from a peer to the local clipboard
    Set { text: String, generation: u64 },
}

/// owns the clipboard polling and broadcasting tasks
pub(crate) struct Clipboard {
    /// `None` if clipboard sync is disabled or unavailable
    request_tx: Option<Sender<ClipboardRequest>>,
    event_rx: Option<Receiver<ClipboardEvent>>,
    share_tx: Option<Sender<String>>,
    tasks: Vec<JoinHandle<()>>,
    /// A remote write has been queued but has not yet become observable via
    /// the clipboard tool. This is shared with the task so a poll already in
    /// flight cannot broadcast the old clipboard value back to the peer.
    pending_remote_write: Rc<Cell<Option<u64>>>,
    next_generation: Cell<u64>,
}

impl Clipboard {
    /// If `enabled` is false, nothing is ever read, written or shared.
    pub(crate) fn new(
        enabled: bool,
        conn: LanMouseSender,
        client_manager: ClientManager,
        listener: ClipboardSender,
    ) -> Self {
        let pending_remote_write = Rc::new(Cell::new(None));
        let mut clipboard = Self {
            request_tx: None,
            event_rx: None,
            share_tx: None,
            tasks: Vec::new(),
            pending_remote_write: pending_remote_write.clone(),
            next_generation: Cell::new(0),
        };
        if !enabled {
            log::info!("clipboard sync is disabled in the configuration");
            return clipboard;
        }
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let (share_tx, share_rx) = channel();
        clipboard.tasks.push(spawn_local(
            ClipboardTask {
                request_rx,
                event_tx,
                tracker: Default::default(),
                pending_remote_write,
            }
            .run(),
        ));
        clipboard.tasks.push(spawn_local(broadcast_task(
            share_rx,
            conn,
            client_manager,
            listener,
        )));
        clipboard.request_tx = Some(request_tx);
        clipboard.event_rx = Some(event_rx);
        clipboard.share_tx = Some(share_tx);
        clipboard
    }

    /// apply text received from a peer to the local clipboard
    pub(crate) fn set_text(&self, text: String) {
        let Some(request_tx) = &self.request_tx else {
            return;
        };
        // an empty text carries nothing worth overwriting the clipboard for
        if text.is_empty() {
            return;
        }
        let generation = self.next_generation.get().wrapping_add(1);
        self.next_generation.set(generation);
        self.pending_remote_write.set(Some(generation));
        if request_tx
            .send(ClipboardRequest::Set { text, generation })
            .is_err()
        {
            self.pending_remote_write.set(None);
        }
    }

    /// share text with all peers supporting it
    pub(crate) fn share(&self, text: String) {
        if let Some(share_tx) = &self.share_tx {
            let _ = share_tx.send(text);
        }
    }

    /// Wait for the next event. Never resolves while clipboard sync is
    /// disabled, and after having reported [`ClipboardEvent::Unavailable`] once.
    pub(crate) async fn event(&mut self) -> ClipboardEvent {
        let Some(event_rx) = self.event_rx.as_mut() else {
            return std::future::pending().await;
        };
        match event_rx.recv().await {
            Some(event) => event,
            None => {
                // the clipboard task has exited
                self.event_rx = None;
                self.request_tx = None;
                self.share_tx = None;
                self.pending_remote_write.set(None);
                ClipboardEvent::Unavailable
            }
        }
    }

    pub(crate) async fn terminate(&mut self) {
        if let Some(mut request_tx) = self.request_tx.take() {
            request_tx.close();
        }
        self.share_tx = None;
        for task in self.tasks.drain(..) {
            // the broadcast task may be in the middle of a transfer
            task.abort();
            if let Err(e) = task.await {
                if !e.is_cancelled() {
                    log::debug!("clipboard task: {e}");
                }
            }
        }
    }
}

/// sends clipboard text to the peers, one transfer at a time
async fn broadcast_task(
    mut share_rx: Receiver<String>,
    conn: LanMouseSender,
    client_manager: ClientManager,
    listener: ClipboardSender,
) {
    let mut transfer = 0u32;
    while let Some(mut text) = share_rx.recv().await {
        // only the latest text is of interest
        while let Some(newer) = share_rx.recv().now_or_never() {
            match newer {
                Some(newer) => text = newer,
                None => return,
            }
        }
        let id = transfer;
        transfer = transfer.wrapping_add(1);
        let chunks = clipboard_chunks(id, &text);
        log::debug!("sharing clipboard text ({} bytes)", text.len());
        send_transfer(&chunks, &conn, &client_manager, &listener).await;
    }
}

async fn send_transfer(
    chunks: &[ClipboardChunk],
    conn: &LanMouseSender,
    client_manager: &ClientManager,
    listener: &ClipboardSender,
) {
    let mut targets = Vec::new();
    for handle in client_manager.active_clients() {
        if !conn.is_connected(handle) {
            // A clipboard update is also a useful opportunity to restore a
            // connection after the peer was restarted. This only triggers the
            // connection attempt (deduplicated per handle), the text is not
            // sent as it is not known yet whether the peer supports it.
            let _ = conn.send(conn.hello(), handle).await;
        } else if conn.supports_clipboard(handle) {
            targets.push(handle);
        } else {
            log::debug!("client {handle} does not support clipboard sharing");
        }
    }
    for (i, chunk) in chunks.iter().enumerate() {
        for &handle in &targets {
            let event = ProtoEvent::Clipboard(chunk.clone());
            if let Err(e) = conn.send(event, handle).await {
                log::debug!("failed to send clipboard to client {handle}: {e}");
            }
        }
        // peers that connected to us (listen side)
        listener.send(chunk.clone());
        if (i + 1) % CHUNKS_PER_BURST == 0 {
            tokio::time::sleep(BURST_PAUSE).await;
        }
    }
}

struct ClipboardTask {
    request_rx: Receiver<ClipboardRequest>,
    event_tx: Sender<ClipboardEvent>,
    tracker: ChangeTracker,
    pending_remote_write: Rc<Cell<Option<u64>>>,
}

impl ClipboardTask {
    async fn run(mut self) {
        let Some(tool) = detect_tool().await else {
            log::info!(
                "no clipboard tool found (requires pbcopy, wl-clipboard, \
                 ClipboardNext `cb`, xclip or xsel), clipboard sync is disabled"
            );
            return;
        };

        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // A received clipboard value must win over a due polling tick.
                // Otherwise a poll can observe the old local value and send it
                // back before the remote write is applied.
                biased;
                request = self.request_rx.recv() => match request {
                    Some(ClipboardRequest::Set { text, generation }) => {
                        match tool.set.write(&text).await {
                            Ok(()) => {
                                log::debug!("applied remote clipboard text");
                                self.tracker.remote_written(&text);
                            }
                            Err(e) => log::warn!("clipboard write failed: {e}"),
                        }
                        if self.pending_remote_write.get() == Some(generation) {
                            self.pending_remote_write.set(None);
                        }
                    }
                    None => break,
                },
                _ = interval.tick() => {
                    let output = match tool.get.read().await {
                        Ok(output) => output,
                        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                            log::warn!("clipboard read timed out");
                            continue;
                        }
                        Err(e) => {
                            log::warn!("clipboard read failed: {e}, disabling sync");
                            return;
                        }
                    };
                    // `wl-paste`/`pbpaste` may have started before the
                    // remote write was queued. Its output is stale in
                    // that case; dropping it prevents a feedback loop.
                    if self.pending_remote_write.get().is_some() {
                        log::debug!("discarding clipboard poll while remote write is pending");
                        continue;
                    }
                    // tools exit unsuccessfully if the clipboard is empty or
                    // does not hold text
                    let text = output
                        .status
                        .success()
                        .then(|| String::from_utf8(output.stdout).ok())
                        .flatten();
                    let Some(text) = self.tracker.poll(text) else {
                        continue;
                    };
                    if text.len() > MAX_BROADCAST_SIZE {
                        log::debug!("clipboard text too large, not sharing");
                        continue;
                    }
                    if self.event_tx.send(ClipboardEvent::Changed(text)).is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> Option<String> {
        Some(s.to_owned())
    }

    #[test]
    fn startup_contents_are_not_shared() {
        let mut t = ChangeTracker::default();
        assert_eq!(t.poll(text("old")), None);
        assert_eq!(t.poll(text("old")), None);
        assert_eq!(t.poll(text("new")), text("new"));
    }

    #[test]
    fn copy_after_empty_startup_is_shared() {
        let mut t = ChangeTracker::default();
        assert_eq!(t.poll(None), None);
        assert_eq!(t.poll(text("first")), text("first"));
    }

    #[test]
    fn empty_or_non_text_clipboard_is_not_shared() {
        let mut t = ChangeTracker::default();
        t.poll(text("a"));
        assert_eq!(t.poll(None), None);
        assert_eq!(t.poll(text("")), None);
        // copying the same text again after an image / clearing is a change
        assert_eq!(t.poll(text("a")), text("a"));
    }

    #[test]
    fn shared_text_keeps_trailing_newline() {
        let mut t = ChangeTracker::default();
        t.poll(text("a"));
        assert_eq!(t.poll(text("line\n")), text("line\n"));
    }

    #[test]
    fn trailing_newline_differences_are_not_a_change() {
        let mut t = ChangeTracker::default();
        t.poll(text("a"));
        t.poll(text("b\n"));
        assert_eq!(t.poll(text("b")), None);
        assert_eq!(t.poll(text("b\r\n")), None);
    }

    #[test]
    fn remote_text_is_not_echoed() {
        let mut t = ChangeTracker::default();
        t.poll(text("local"));
        t.remote_written("remote\n");
        assert_eq!(t.poll(text("remote\n")), None);
        assert_eq!(t.poll(text("remote")), None);
    }

    #[test]
    fn recopying_received_text_is_shared_again() {
        let mut t = ChangeTracker::default();
        t.poll(text("local"));
        t.remote_written("X");
        assert_eq!(t.poll(text("X")), None);
        assert_eq!(t.poll(text("Y")), text("Y"));
        assert_eq!(t.poll(text("X")), text("X"));
    }

    fn clipboard_with_event_channel() -> (Clipboard, Sender<ClipboardEvent>) {
        let (event_tx, event_rx) = channel();
        let clipboard = Clipboard {
            request_tx: None,
            event_rx: Some(event_rx),
            share_tx: None,
            tasks: Vec::new(),
            pending_remote_write: Default::default(),
            next_generation: Cell::new(0),
        };
        (clipboard, event_tx)
    }

    /// If the clipboard task exits (no tool available) the event stream must
    /// not resolve over and over, this would spin the service main loop.
    #[tokio::test]
    async fn unavailable_is_reported_once() {
        let (mut clipboard, event_tx) = clipboard_with_event_channel();
        drop(event_tx);
        assert!(matches!(
            clipboard.event().await,
            ClipboardEvent::Unavailable
        ));
        let again = tokio::time::timeout(Duration::from_millis(50), clipboard.event()).await;
        assert!(again.is_err(), "event() must stay pending once unavailable");
    }

    #[tokio::test]
    async fn disabled_clipboard_never_reports_events() {
        let (mut clipboard, _event_tx) = clipboard_with_event_channel();
        clipboard.event_rx = None;
        let event = tokio::time::timeout(Duration::from_millis(50), clipboard.event()).await;
        assert!(event.is_err());
        // applying remote text is a no-op
        clipboard.set_text("text".into());
        assert_eq!(clipboard.pending_remote_write.get(), None);
    }

    #[tokio::test]
    async fn argument_text_is_validated() {
        let cmd = CliCommand::set("cb", &["copy", "-n"], true);
        let err = cmd.write("--help").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let err = cmd
            .write(&"a".repeat(MAX_ARG_TEXT_SIZE + 1))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn hung_tool_times_out() {
        let cmd = CliCommand::get("sleep", &["30"]);
        let err = cmd.read().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn failing_tool_is_an_error() {
        let cmd = CliCommand::set("false", &[], false);
        assert!(cmd.write("text").await.is_err());
    }
}
