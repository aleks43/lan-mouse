use local_channel::mpsc::{Receiver, Sender, channel};
use std::{cell::Cell, rc::Rc, time::Duration};
use tokio::process::Command;
use tokio::task::{JoinHandle, spawn_local};

/// how often the local clipboard is polled for changes
const POLL_INTERVAL: Duration = Duration::from_millis(300);

/// clipboard text larger than this is never broadcast
const MAX_BROADCAST_SIZE: usize = 1024 * 1024;

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
                // pbpaste has no version flag. Invoking it without arguments
                // is both its normal read operation and an availability check.
                "pbpaste" => &[],
                _ => &["--version"],
            },
            text_arg: false,
            text_stdin: false,
        }
    }

    async fn run(&self, text: Option<&str>) -> std::io::Result<std::process::Output> {
        let mut cmd = Command::new(self.program);
        cmd.args(self.args);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());
        if self.text_arg {
            cmd.arg(text.unwrap_or_default());
        } else if self.text_stdin {
            use tokio::io::AsyncWriteExt;
            cmd.stdin(std::process::Stdio::piped());
            let mut child = cmd.spawn()?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(text.unwrap_or_default().as_bytes()).await?;
                stdin.flush().await?;
                drop(stdin);
            }
            return child.wait_with_output().await;
        }
        cmd.output().await
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
            get: CliCommand::get("wl-paste", &["-n"]),
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
            // Do not fall back to X11's legacy STRING target: it is
            // single-byte text and turns UTF-8 Cyrillic into mojibake on a
            // receiving native clipboard (such as macOS pasteboard).
            get: CliCommand::get(
                "xclip",
                &["-selection", "clipboard", "-o", "-target", "UTF8_STRING"],
            ),
            set: CliCommand::set(
                "xclip",
                &["-selection", "clipboard", "-target", "UTF8_STRING"],
                false,
            ),
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
        match tool.get.probe().run(None).await {
            Ok(output) if output.status.success() => {
                log::info!("clipboard sync enabled, backend: {}", tool.name);
                return Some(tool);
            }
            _ => log::debug!("clipboard backend `{}` not available", tool.name),
        }
    }
    None
}

/// normalize text read from the clipboard so that round-trips through the
/// various tools (which differ in trailing newline handling) compare equal
fn normalize(mut text: String) -> String {
    while text.ends_with('\n') || text.ends_with('\r') {
        text.pop();
    }
    text
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

/// owns the clipboard polling task
pub(crate) struct Clipboard {
    request_tx: Sender<ClipboardRequest>,
    event_rx: Receiver<ClipboardEvent>,
    task: JoinHandle<()>,
    disabled: bool,
    /// A remote write has been queued but has not yet become observable via
    /// the clipboard tool. This is shared with the task so a poll already in
    /// flight cannot broadcast the old clipboard value back to the peer.
    pending_remote_write: Rc<Cell<Option<u64>>>,
    next_generation: Cell<u64>,
}

impl Clipboard {
    pub(crate) fn new() -> Self {
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let pending_remote_write = Rc::new(Cell::new(None));
        let task = spawn_local(
            ClipboardTask {
                request_rx,
                event_tx,
                last_seen: None,
                last_set: None,
                pending_remote_write: pending_remote_write.clone(),
            }
            .run(),
        );
        Self {
            request_tx,
            event_rx,
            task,
            disabled: false,
            pending_remote_write,
            next_generation: Cell::new(0),
        }
    }

    pub(crate) fn set_text(&self, text: String) {
        if self.disabled {
            return;
        }
        let generation = self.next_generation.get().wrapping_add(1);
        self.next_generation.set(generation);
        self.pending_remote_write.set(Some(generation));
        if self
            .request_tx
            .send(ClipboardRequest::Set { text, generation })
            .is_err()
        {
            self.pending_remote_write.set(None);
        }
    }

    pub(crate) async fn event(&mut self) -> ClipboardEvent {
        match self.event_rx.recv().await {
            Some(event) => event,
            None => {
                self.disabled = true;
                ClipboardEvent::Unavailable
            }
        }
    }

    pub(crate) async fn terminate(&mut self) {
        self.request_tx.close();
        if let Err(e) = (&mut self.task).await {
            log::debug!("clipboard task: {e}");
        }
    }
}

struct ClipboardTask {
    request_rx: Receiver<ClipboardRequest>,
    event_tx: Sender<ClipboardEvent>,
    /// last text observed on the clipboard
    last_seen: Option<String>,
    /// last text applied from a remote peer
    last_set: Option<String>,
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
                        match tool.set.run(Some(&text)).await {
                            Ok(_) => {
                                log::debug!("applied remote clipboard text");
                                self.last_set = Some(text.clone());
                                self.last_seen = Some(text);
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
                    match tool.get.run(None).await {
                        Ok(output) => {
                            // `wl-paste`/`pbpaste` may have started before the
                            // remote write was queued. Its output is stale in
                            // that case; dropping it prevents a feedback loop.
                            if self.pending_remote_write.get().is_some() {
                                log::debug!("discarding clipboard poll while remote write is pending");
                                continue;
                            }
                            let text = normalize(String::from_utf8_lossy(&output.stdout).into_owned());
                            if Some(&text) == self.last_seen.as_ref() {
                                continue;
                            }
                            self.last_seen = Some(text.clone());
                            // suppress the echo of a text we applied ourselves
                            if Some(&text) == self.last_set.as_ref() {
                                continue;
                            }
                            if text.len() > MAX_BROADCAST_SIZE {
                                log::debug!("clipboard text too large, not sharing");
                                continue;
                            }
                            self.event_tx
                                .send(ClipboardEvent::Changed(text))
                                .expect("channel closed");
                        }
                        Err(e) => {
                            log::warn!("clipboard read failed: {e}, disabling sync");
                            return;
                        }
                    }
                }
            }
        }
    }
}
