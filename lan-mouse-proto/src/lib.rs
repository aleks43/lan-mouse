use input_event::{Event as InputEvent, KeyboardEvent, PointerEvent};
use num_enum::{IntoPrimitive, TryFromPrimitive, TryFromPrimitiveError};
use paste::paste;
use std::{
    fmt::{Debug, Display, Formatter},
    mem::size_of,
};
use thiserror::Error;

/// maximum payload size of a single [`ProtoEvent::Clipboard`] chunk
pub const CLIPBOARD_CHUNK_SIZE: usize = 1024;

/// maximum size of a reassembled clipboard transfer
pub const MAX_CLIPBOARD_SIZE: usize = 1024 * 1024;

/// defines the maximum size an encoded event can take up
/// this is currently the clipboard chunk event
/// type: u8, transfer: u32, index: u32, last: u8, len: u16, data: [u8; CLIPBOARD_CHUNK_SIZE]
pub const MAX_EVENT_SIZE: usize = size_of::<u8>()
    + 2 * size_of::<u32>()
    + size_of::<u8>()
    + size_of::<u16>()
    + CLIPBOARD_CHUNK_SIZE;

/// error type for protocol violations
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// event type does not exist
    #[error("invalid event id: `{0}`")]
    InvalidEventId(#[from] TryFromPrimitiveError<EventType>),
    /// position type does not exist
    #[error("invalid event id: `{0}`")]
    InvalidPosition(#[from] TryFromPrimitiveError<Position>),
    /// clipboard chunk declares more data than the datagram contains
    #[error("clipboard chunk length exceeds datagram size")]
    InvalidClipboardLength,
}

/// Position of a client
#[derive(Clone, Copy, Debug, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum Position {
    Left,
    Right,
    Top,
    Bottom,
}

impl Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let pos = match self {
            Position::Left => "left",
            Position::Right => "right",
            Position::Top => "top",
            Position::Bottom => "bottom",
        };
        write!(f, "{pos}")
    }
}

/// a fragment of a clipboard text transfer
///
/// A text is split into chunks of at most [`CLIPBOARD_CHUNK_SIZE`] bytes
/// (on UTF-8 boundaries). Chunks of one transfer share a `transfer` id and
/// are numbered by `index`. The chunk with `last == true` terminates the
/// transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardChunk {
    /// id distinguishing concurrent/sequential transfers
    pub transfer: u32,
    /// index of this chunk within the transfer, starting at 0
    pub index: u32,
    /// true if this is the last chunk of the transfer
    pub last: bool,
    /// raw UTF-8 payload fragment
    pub data: Vec<u8>,
}

/// collects [`ClipboardChunk`]s and reconstructs the transferred text
#[derive(Default)]
pub struct ClipboardAssembly {
    transfer: Option<u32>,
    next_index: u32,
    buf: Vec<u8>,
}

/// errors that invalidate a clipboard transfer
#[derive(Debug, Error)]
pub enum ClipboardAssemblyError {
    #[error("clipboard chunk exceeds `{CLIPBOARD_CHUNK_SIZE}` bytes")]
    ChunkTooLarge,
    #[error("out of order clipboard chunk, dropping transfer")]
    OutOfOrder,
    #[error("clipboard transfer exceeds `{MAX_CLIPBOARD_SIZE}` bytes, dropping transfer")]
    TransferTooLarge,
    #[error("clipboard text is not valid UTF-8")]
    InvalidUtf8,
}

impl ClipboardAssembly {
    /// feed a received chunk, returns the complete text if the transfer finished
    pub fn handle(
        &mut self,
        chunk: ClipboardChunk,
    ) -> Result<Option<String>, ClipboardAssemblyError> {
        if chunk.data.len() > CLIPBOARD_CHUNK_SIZE {
            self.reset();
            return Err(ClipboardAssemblyError::ChunkTooLarge);
        }

        // a new transfer id always starts a fresh assembly
        if self.transfer != Some(chunk.transfer) {
            self.reset();
            self.transfer = Some(chunk.transfer);
        }

        if chunk.index != self.next_index {
            self.reset();
            return Err(ClipboardAssemblyError::OutOfOrder);
        }

        if self.buf.len() + chunk.data.len() > MAX_CLIPBOARD_SIZE {
            self.reset();
            return Err(ClipboardAssemblyError::TransferTooLarge);
        }

        self.buf.extend_from_slice(&chunk.data);
        if !chunk.last {
            self.next_index += 1;
            return Ok(None);
        }

        let transfer = std::mem::take(&mut self.buf);
        self.transfer = None;
        self.next_index = 0;
        match String::from_utf8(transfer) {
            Ok(text) => Ok(Some(text)),
            Err(_) => Err(ClipboardAssemblyError::InvalidUtf8),
        }
    }

    fn reset(&mut self) {
        self.transfer = None;
        self.next_index = 0;
        self.buf.clear();
    }
}

/// split clipboard text into protocol chunks ready to be sent
pub fn clipboard_chunks(transfer: u32, text: &str) -> Vec<ClipboardChunk> {
    let bytes = text.as_bytes();
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut index = 0;
    loop {
        let mut end = (start + CLIPBOARD_CHUNK_SIZE).min(bytes.len());
        // never split a UTF-8 codepoint
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let last = end == bytes.len();
        chunks.push(ClipboardChunk {
            transfer,
            index,
            last,
            data: bytes[start..end].to_vec(),
        });
        if last {
            return chunks;
        }
        start = end;
        index += 1;
    }
}

/// main lan-mouse protocol event type
#[derive(Clone, Debug)]
pub enum ProtoEvent {
    /// notify a client that the cursor entered its region at the given position
    /// [`ProtoEvent::Ack`] with the same serial is used for synchronization between devices
    Enter(Position),
    /// notify a client that the cursor left its region
    /// [`ProtoEvent::Ack`] with the same serial is used for synchronization between devices
    Leave(u32),
    /// acknowledge of an [`ProtoEvent::Enter`] or [`ProtoEvent::Leave`] event
    Ack(u32),
    /// Input event
    Input(InputEvent),
    /// Ping event for tracking unresponsive clients.
    /// A client has to respond with [`ProtoEvent::Pong`].
    Ping,
    /// Response to [`ProtoEvent::Ping`], true if emulation is enabled / available
    Pong(bool),
    /// Build identification for the sending peer. Sent by the
    /// connect side once after the connection authenticates, and
    /// echoed back by the listen side in reply, so each end can
    /// display the peer's build hash and warn (soft) on mismatch.
    /// `commit` is the 8-byte ASCII short commit hash from
    /// `shadow_rs`'s `SHORT_COMMIT`. Old peers that don't
    /// recognize the event type silently skip it per the
    /// forward-compat handling in the receive loop.
    Hello { commit: [u8; 8] },
    /// a chunk of clipboard text, see [`ClipboardChunk`] and [`clipboard_chunks`].
    /// Sent by a peer that wants to share its clipboard contents. The
    /// receiving side reassembles chunks with [`ClipboardAssembly`].
    Clipboard(ClipboardChunk),
}

impl Display for ProtoEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoEvent::Enter(s) => write!(f, "Enter({s})"),
            ProtoEvent::Leave(s) => write!(f, "Leave({s})"),
            ProtoEvent::Ack(s) => write!(f, "Ack({s})"),
            ProtoEvent::Input(e) => write!(f, "{e}"),
            ProtoEvent::Ping => write!(f, "ping"),
            ProtoEvent::Pong(alive) => {
                write!(
                    f,
                    "pong: {}",
                    if *alive { "alive" } else { "not available" }
                )
            }
            ProtoEvent::Hello { commit } => {
                let s = std::str::from_utf8(commit).unwrap_or("????????");
                write!(f, "Hello({s})")
            }
            ProtoEvent::Clipboard(c) => write!(
                f,
                "Clipboard(transfer: {}, chunk: {}, {} bytes{})",
                c.transfer,
                c.index,
                c.data.len(),
                if c.last { ", last" } else { "" }
            ),
        }
    }
}

#[derive(TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum EventType {
    PointerMotion,
    PointerButton,
    PointerAxis,
    PointerAxisValue120,
    KeyboardKey,
    KeyboardModifiers,
    Ping,
    Pong,
    Enter,
    Leave,
    Ack,
    Hello,
    Clipboard,
}

impl ProtoEvent {
    fn event_type(&self) -> EventType {
        match self {
            ProtoEvent::Input(e) => match e {
                InputEvent::Pointer(p) => match p {
                    PointerEvent::Motion { .. } => EventType::PointerMotion,
                    PointerEvent::Button { .. } => EventType::PointerButton,
                    PointerEvent::Axis { .. } => EventType::PointerAxis,
                    PointerEvent::AxisDiscrete120 { .. } => EventType::PointerAxisValue120,
                },
                InputEvent::Keyboard(k) => match k {
                    KeyboardEvent::Key { .. } => EventType::KeyboardKey,
                    KeyboardEvent::Modifiers { .. } => EventType::KeyboardModifiers,
                },
            },
            ProtoEvent::Ping => EventType::Ping,
            ProtoEvent::Pong(_) => EventType::Pong,
            ProtoEvent::Enter(_) => EventType::Enter,
            ProtoEvent::Leave(_) => EventType::Leave,
            ProtoEvent::Ack(_) => EventType::Ack,
            ProtoEvent::Hello { .. } => EventType::Hello,
            ProtoEvent::Clipboard(_) => EventType::Clipboard,
        }
    }
}

impl TryFrom<[u8; MAX_EVENT_SIZE]> for ProtoEvent {
    type Error = ProtocolError;

    fn try_from(buf: [u8; MAX_EVENT_SIZE]) -> Result<Self, Self::Error> {
        let mut buf = &buf[..];
        let event_type = decode_u8(&mut buf)?;
        match EventType::try_from(event_type)? {
            EventType::PointerMotion => {
                Ok(Self::Input(InputEvent::Pointer(PointerEvent::Motion {
                    time: decode_u32(&mut buf)?,
                    dx: decode_f64(&mut buf)?,
                    dy: decode_f64(&mut buf)?,
                })))
            }
            EventType::PointerButton => {
                Ok(Self::Input(InputEvent::Pointer(PointerEvent::Button {
                    time: decode_u32(&mut buf)?,
                    button: decode_u32(&mut buf)?,
                    state: decode_u32(&mut buf)?,
                })))
            }
            EventType::PointerAxis => Ok(Self::Input(InputEvent::Pointer(PointerEvent::Axis {
                time: decode_u32(&mut buf)?,
                axis: decode_u8(&mut buf)?,
                value: decode_f64(&mut buf)?,
            }))),
            EventType::PointerAxisValue120 => Ok(Self::Input(InputEvent::Pointer(
                PointerEvent::AxisDiscrete120 {
                    axis: decode_u8(&mut buf)?,
                    value: decode_i32(&mut buf)?,
                },
            ))),
            EventType::KeyboardKey => Ok(Self::Input(InputEvent::Keyboard(KeyboardEvent::Key {
                time: decode_u32(&mut buf)?,
                key: decode_u32(&mut buf)?,
                state: decode_u8(&mut buf)?,
            }))),
            EventType::KeyboardModifiers => Ok(Self::Input(InputEvent::Keyboard(
                KeyboardEvent::Modifiers {
                    depressed: decode_u32(&mut buf)?,
                    latched: decode_u32(&mut buf)?,
                    locked: decode_u32(&mut buf)?,
                    group: decode_u32(&mut buf)?,
                },
            ))),
            EventType::Ping => Ok(Self::Ping),
            EventType::Pong => Ok(Self::Pong(decode_u8(&mut buf)? != 0)),
            EventType::Enter => Ok(Self::Enter(decode_u8(&mut buf)?.try_into()?)),
            EventType::Leave => Ok(Self::Leave(decode_u32(&mut buf)?)),
            EventType::Ack => Ok(Self::Ack(decode_u32(&mut buf)?)),
            EventType::Hello => {
                let mut commit = [0u8; 8];
                for b in commit.iter_mut() {
                    *b = decode_u8(&mut buf)?;
                }
                Ok(Self::Hello { commit })
            }
            EventType::Clipboard => {
                let transfer = decode_u32(&mut buf)?;
                let index = decode_u32(&mut buf)?;
                let last = decode_u8(&mut buf)? != 0;
                let len = decode_u16(&mut buf)? as usize;
                if len > buf.len() {
                    return Err(ProtocolError::InvalidClipboardLength);
                }
                let mut data = Vec::with_capacity(len);
                data.extend_from_slice(&buf[..len]);
                Ok(Self::Clipboard(ClipboardChunk {
                    transfer,
                    index,
                    last,
                    data,
                }))
            }
        }
    }
}

impl From<ProtoEvent> for ([u8; MAX_EVENT_SIZE], usize) {
    fn from(event: ProtoEvent) -> Self {
        let mut buf = [0u8; MAX_EVENT_SIZE];
        let mut len = 0usize;
        {
            let mut buf = &mut buf[..];
            let buf = &mut buf;
            let len = &mut len;
            encode_u8(buf, len, event.event_type() as u8);
            match event {
                ProtoEvent::Input(event) => match event {
                    InputEvent::Pointer(p) => match p {
                        PointerEvent::Motion { time, dx, dy } => {
                            encode_u32(buf, len, time);
                            encode_f64(buf, len, dx);
                            encode_f64(buf, len, dy);
                        }
                        PointerEvent::Button {
                            time,
                            button,
                            state,
                        } => {
                            encode_u32(buf, len, time);
                            encode_u32(buf, len, button);
                            encode_u32(buf, len, state);
                        }
                        PointerEvent::Axis { time, axis, value } => {
                            encode_u32(buf, len, time);
                            encode_u8(buf, len, axis);
                            encode_f64(buf, len, value);
                        }
                        PointerEvent::AxisDiscrete120 { axis, value } => {
                            encode_u8(buf, len, axis);
                            encode_i32(buf, len, value);
                        }
                    },
                    InputEvent::Keyboard(k) => match k {
                        KeyboardEvent::Key { time, key, state } => {
                            encode_u32(buf, len, time);
                            encode_u32(buf, len, key);
                            encode_u8(buf, len, state);
                        }
                        KeyboardEvent::Modifiers {
                            depressed,
                            latched,
                            locked,
                            group,
                        } => {
                            encode_u32(buf, len, depressed);
                            encode_u32(buf, len, latched);
                            encode_u32(buf, len, locked);
                            encode_u32(buf, len, group);
                        }
                    },
                },
                ProtoEvent::Ping => {}
                ProtoEvent::Pong(alive) => encode_u8(buf, len, alive as u8),
                ProtoEvent::Enter(pos) => encode_u8(buf, len, pos as u8),
                ProtoEvent::Leave(serial) => encode_u32(buf, len, serial),
                ProtoEvent::Ack(serial) => encode_u32(buf, len, serial),
                ProtoEvent::Hello { commit } => {
                    for b in commit.iter() {
                        encode_u8(buf, len, *b);
                    }
                }
                ProtoEvent::Clipboard(chunk) => {
                    debug_assert!(chunk.data.len() <= CLIPBOARD_CHUNK_SIZE);
                    encode_u32(buf, len, chunk.transfer);
                    encode_u32(buf, len, chunk.index);
                    encode_u8(buf, len, chunk.last as u8);
                    encode_u16(buf, len, chunk.data.len() as u16);
                    for b in chunk.data.iter() {
                        encode_u8(buf, len, *b);
                    }
                }
            }
        }
        (buf, len)
    }
}

macro_rules! decode_impl {
    ($t:ty) => {
        paste! {
            fn [<decode_ $t>](data: &mut &[u8]) -> Result<$t, ProtocolError> {
                let (int_bytes, rest) = data.split_at(size_of::<$t>());
                *data = rest;
                Ok($t::from_be_bytes(int_bytes.try_into().unwrap()))
            }
        }
    };
}

decode_impl!(u8);
decode_impl!(u16);
decode_impl!(u32);
decode_impl!(i32);
decode_impl!(f64);

macro_rules! encode_impl {
    ($t:ty) => {
        paste! {
            fn [<encode_ $t>](buf: &mut &mut [u8], amt: &mut usize, n: $t) {
                let src = n.to_be_bytes();
                let data = std::mem::take(buf);
                let (int_bytes, rest) = data.split_at_mut(size_of::<$t>());
                int_bytes.copy_from_slice(&src);
                *amt += size_of::<$t>();
                *buf = rest
            }
        }
    };
}

encode_impl!(u8);
encode_impl!(u16);
encode_impl!(u32);
encode_impl!(i32);
encode_impl!(f64);

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(event: ProtoEvent) -> Result<ProtoEvent, ProtocolError> {
        let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
        assert!(len <= MAX_EVENT_SIZE);
        ProtoEvent::try_from(buf)
    }

    #[test]
    fn clipboard_chunk_roundtrip() {
        let chunk = ClipboardChunk {
            transfer: 7,
            index: 3,
            last: true,
            data: b"hello".to_vec(),
        };
        let decoded = roundtrip(ProtoEvent::Clipboard(chunk.clone())).expect("decode");
        match decoded {
            ProtoEvent::Clipboard(c) => assert_eq!(c, chunk),
            _ => panic!("unexpected event type"),
        }
    }

    #[test]
    fn clipboard_chunk_length_overflow_is_rejected() {
        let (mut buf, _): ([u8; MAX_EVENT_SIZE], usize) = ProtoEvent::Clipboard(ClipboardChunk {
            transfer: 0,
            index: 0,
            last: false,
            data: b"abc".to_vec(),
        })
        .into();
        // patch the declared payload length (offset: 1 + 4 + 4 + 1 = 10)
        // to exceed the datagram size
        let inflated = 60000u16.to_be_bytes();
        buf[10] = inflated[0];
        buf[11] = inflated[1];
        assert!(matches!(
            ProtoEvent::try_from(buf),
            Err(ProtocolError::InvalidClipboardLength)
        ));
    }

    #[test]
    fn chunk_split_join_ascii() {
        let text = "a".repeat(CLIPBOARD_CHUNK_SIZE * 2 + 5);
        let chunks = clipboard_chunks(1, &text);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.last().unwrap().last);
        let mut assembly = ClipboardAssembly::default();
        let mut result = None;
        for chunk in chunks {
            if let Some(text) = assembly.handle(chunk).unwrap() {
                result = Some(text);
            }
        }
        assert_eq!(result.as_deref(), Some(text.as_str()));
    }

    #[test]
    fn chunk_split_never_breaks_codepoints() {
        // 3-byte codepoints around the chunk boundary
        let text = "ä".repeat(1000);
        let chunks = clipboard_chunks(2, &text);
        let mut assembly = ClipboardAssembly::default();
        let mut result = None;
        for chunk in chunks {
            if let Some(t) = assembly.handle(chunk).unwrap() {
                result = Some(t);
            }
        }
        assert_eq!(result.as_deref(), Some(text.as_str()));
    }

    #[test]
    fn empty_text_is_one_final_chunk() {
        let chunks = clipboard_chunks(3, "");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].last);
        let mut assembly = ClipboardAssembly::default();
        let text = assembly.handle(chunks[0].clone()).unwrap();
        assert_eq!(text.as_deref(), Some(""));
    }

    #[test]
    fn out_of_order_chunk_drops_transfer() {
        let chunks = clipboard_chunks(4, &"b".repeat(CLIPBOARD_CHUNK_SIZE + 1));
        let mut assembly = ClipboardAssembly::default();
        // index 1 cannot be the first chunk of a transfer
        let err = assembly.handle(chunks[1].clone()).unwrap_err();
        assert!(matches!(err, ClipboardAssemblyError::OutOfOrder));
        // the assembly was reset, replaying the transfer works
        assembly.handle(chunks[0].clone()).unwrap();
        let text = assembly.handle(chunks[1].clone()).unwrap();
        assert_eq!(
            text.as_deref(),
            Some(&"b".repeat(CLIPBOARD_CHUNK_SIZE + 1)[..])
        );
    }

    #[test]
    fn new_transfer_id_resets_assembly() {
        let mut assembly = ClipboardAssembly::default();
        let mut chunks = clipboard_chunks(5, &"c".repeat(CLIPBOARD_CHUNK_SIZE + 1));
        assembly.handle(chunks.remove(0)).unwrap();
        let text = assembly
            .handle(ClipboardChunk {
                transfer: 6,
                index: 0,
                last: true,
                data: b"fresh".to_vec(),
            })
            .unwrap();
        assert_eq!(text.as_deref(), Some("fresh"));
    }

    #[test]
    fn oversized_transfer_is_dropped() {
        let big = "d".repeat(CLIPBOARD_CHUNK_SIZE);
        let big = big.as_bytes();
        let mut assembly = ClipboardAssembly::default();
        let mut index = 0;
        let mut result = None;
        while result.is_none() {
            let chunk = ClipboardChunk {
                transfer: 0,
                index,
                last: false,
                data: big.to_vec(),
            };
            index += 1;
            match assembly.handle(chunk) {
                Ok(t) => result = t,
                Err(ClipboardAssemblyError::TransferTooLarge) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        // assembly was reset, a new transfer works again
        let text = assembly
            .handle(ClipboardChunk {
                transfer: 1,
                index: 0,
                last: true,
                data: vec![],
            })
            .unwrap();
        assert_eq!(text.as_deref(), Some(""));
    }
}
