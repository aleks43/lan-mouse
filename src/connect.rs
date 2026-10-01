use crate::client::ClientManager;
use crate::config::local_commit;
use lan_mouse_ipc::{ClientHandle, DEFAULT_PORT};
use lan_mouse_proto::{CAP_CLIPBOARD, MAX_EVENT_SIZE, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::Mutex,
    task::{JoinSet, spawn_local},
};
use webrtc_dtls::{
    config::{Config, ExtendedMasterSecretType},
    conn::DTLSConn,
    crypto::Certificate,
};
use webrtc_util::Conn;

#[derive(Debug, Error)]
pub(crate) enum LanMouseConnectionError {
    #[error(transparent)]
    Bind(#[from] io::Error),
    #[error(transparent)]
    Dtls(#[from] webrtc_dtls::Error),
    #[error(transparent)]
    Webrtc(#[from] webrtc_util::Error),
    #[error("not connected")]
    NotConnected,
    #[error("emulation is disabled on the target device")]
    TargetEmulationDisabled,
    #[error("Connection timed out")]
    Timeout,
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

async fn connect(
    addr: SocketAddr,
    cert: Certificate,
) -> Result<(Arc<dyn Conn + Sync + Send>, SocketAddr), (SocketAddr, LanMouseConnectionError)> {
    log::info!("connecting to {addr} ...");
    let conn = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| (addr, e.into()))?,
    );
    conn.connect(addr).await.map_err(|e| (addr, e.into()))?;
    let config = Config {
        certificates: vec![cert],
        server_name: "ignored".to_owned(),
        insecure_skip_verify: true,
        extended_master_secret: ExtendedMasterSecretType::Require,
        ..Default::default()
    };
    let timeout = tokio::time::sleep(DEFAULT_CONNECTION_TIMEOUT);
    tokio::select! {
        _ = timeout => Err((addr, LanMouseConnectionError::Timeout)),
        result = DTLSConn::new(conn, config, true, None) => match result {
            Ok(dtls_conn) => Ok((Arc::new(dtls_conn), addr)),
            Err(e) => Err((addr, e.into())),
        }
    }
}

async fn connect_any(
    addrs: &[SocketAddr],
    cert: Certificate,
) -> Result<(Arc<dyn Conn + Send + Sync>, SocketAddr), LanMouseConnectionError> {
    let mut joinset = JoinSet::new();
    for &addr in addrs {
        joinset.spawn_local(connect(addr, cert.clone()));
    }
    loop {
        match joinset.join_next().await {
            None => return Err(LanMouseConnectionError::NotConnected),
            Some(r) => match r.expect("join error") {
                Ok(conn) => return Ok(conn),
                Err((a, e)) => {
                    log::warn!("failed to connect to {a}: `{e}`")
                }
            },
        };
    }
}

pub(crate) struct LanMouseConnection {
    sender: LanMouseSender,
    recv_rx: Receiver<(ClientHandle, ProtoEvent)>,
}

/// Cloneable sending half of a [`LanMouseConnection`].
/// Allows multiple owners (capture task, clipboard sync) to send
/// events to the configured clients while only one owns the
/// receive side.
#[derive(Clone)]
pub(crate) struct LanMouseSender {
    cert: Certificate,
    client_manager: ClientManager,
    conns: Rc<Mutex<HashMap<SocketAddr, Arc<dyn Conn + Send + Sync>>>>,
    connecting: Rc<Mutex<HashSet<ClientHandle>>>,
    recv_tx: Sender<(ClientHandle, ProtoEvent)>,
    ping_response: Rc<RefCell<HashSet<SocketAddr>>>,
    /// capability flags advertised to peers in our `Hello`
    caps: u8,
    /// clients whose `Hello` advertised [`CAP_CLIPBOARD`]
    clipboard_peers: Rc<RefCell<HashSet<ClientHandle>>>,
}

impl LanMouseSender {
    fn new(
        cert: Certificate,
        client_manager: ClientManager,
        recv_tx: Sender<(ClientHandle, ProtoEvent)>,
        caps: u8,
    ) -> Self {
        Self {
            caps,
            clipboard_peers: Default::default(),
            cert,
            client_manager,
            conns: Default::default(),
            connecting: Default::default(),
            recv_tx,
            ping_response: Default::default(),
        }
    }

    pub(crate) async fn send(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        if let Some(addr) = self.client_manager.active_addr(handle) {
            let conn = {
                let conns = self.conns.lock().await;
                conns.get(&addr).cloned()
            };
            if let Some(conn) = conn {
                if !self.client_manager.alive(handle) {
                    return Err(LanMouseConnectionError::TargetEmulationDisabled);
                }
                log::trace!("{event} >->->->->- {addr}");
                let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = event.into();
                let buf = &buf[..len];
                match conn.send(buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("client {handle} failed to send: {e}");
                        disconnect(
                            &self.client_manager,
                            handle,
                            addr,
                            &self.conns,
                            &self.clipboard_peers,
                        )
                        .await;
                    }
                }
                return Ok(());
            }
        }

        // check if we are already trying to connect
        let mut connecting = self.connecting.lock().await;
        if !connecting.contains(&handle) {
            connecting.insert(handle);
            // connect in the background
            spawn_local(self.clone().connect_to_handle(handle));
        }
        Err(LanMouseConnectionError::NotConnected)
    }

    /// whether the client announced support for clipboard transfers.
    /// Peers predating clipboard support drop the connection on receiving
    /// clipboard datagrams, so nothing may be sent to them.
    pub(crate) fn supports_clipboard(&self, handle: ClientHandle) -> bool {
        self.clipboard_peers.borrow().contains(&handle)
    }

    /// whether a connection to the client is currently established
    pub(crate) fn is_connected(&self, handle: ClientHandle) -> bool {
        self.client_manager.active_addr(handle).is_some()
    }

    /// the capability-carrying hello message sent after connecting
    pub(crate) fn hello(&self) -> ProtoEvent {
        ProtoEvent::Hello {
            commit: local_commit(),
            caps: self.caps,
        }
    }

    async fn connect_to_handle(self, handle: ClientHandle) -> Result<(), LanMouseConnectionError> {
        log::info!("client {handle} connecting ...");
        // sending did not work, figure out active conn.
        if let Some(addrs) = self.client_manager.get_ips(handle) {
            let port = self.client_manager.get_port(handle).unwrap_or(DEFAULT_PORT);
            let addrs = addrs
                .into_iter()
                .map(|a| SocketAddr::new(a, port))
                .collect::<Vec<_>>();
            log::info!("client ({handle}) connecting ... (ips: {addrs:?})");
            let res = connect_any(&addrs, self.cert.clone()).await;
            let (conn, addr) = match res {
                Ok(c) => c,
                Err(e) => {
                    self.connecting.lock().await.remove(&handle);
                    return Err(e);
                }
            };
            log::info!("client ({handle}) connected @ {addr}");
            self.client_manager.set_active_addr(handle, Some(addr));
            self.conns.lock().await.insert(addr, conn.clone());
            self.connecting.lock().await.remove(&handle);

            // Best-effort version handshake. Send our commit hash once
            // immediately after the DTLS handshake; the listen side
            // mirrors a Hello back so the receive loop can populate
            // `peer_commit`. Old peers will silently skip this event
            // per the forward-compat handler in [`receive_loop`].
            let (buf, len): ([u8; MAX_EVENT_SIZE], usize) = self.hello().into();
            if let Err(e) = conn.send(&buf[..len]).await {
                log::debug!("hello send to {addr} failed: {e}");
            }

            // poll connection for active
            spawn_local(ping_pong(addr, conn.clone(), self.ping_response.clone()));

            // receiver
            spawn_local(receive_loop(self.clone(), handle, addr, conn));
            return Ok(());
        }
        self.connecting.lock().await.remove(&handle);
        Err(LanMouseConnectionError::NotConnected)
    }
}

impl LanMouseConnection {
    pub(crate) fn new(cert: Certificate, client_manager: ClientManager, caps: u8) -> Self {
        let (recv_tx, recv_rx) = channel();
        let sender = LanMouseSender::new(cert, client_manager, recv_tx, caps);
        Self { sender, recv_rx }
    }

    /// whether clipboard transfers are enabled locally
    pub(crate) fn clipboard_enabled(&self) -> bool {
        self.sender.caps & CAP_CLIPBOARD != 0
    }

    pub(crate) fn sender(&self) -> LanMouseSender {
        self.sender.clone()
    }

    pub(crate) async fn recv(&mut self) -> (ClientHandle, ProtoEvent) {
        self.recv_rx.recv().await.expect("channel closed")
    }

    pub(crate) async fn send(
        &self,
        event: ProtoEvent,
        handle: ClientHandle,
    ) -> Result<(), LanMouseConnectionError> {
        self.sender.send(event, handle).await
    }
}

async fn ping_pong(
    addr: SocketAddr,
    conn: Arc<dyn Conn + Send + Sync>,
    ping_response: Rc<RefCell<HashSet<SocketAddr>>>,
) {
    loop {
        let (buf, len) = ProtoEvent::Ping.into();

        // send 4 pings, at least one must be answered
        for _ in 0..4 {
            if let Err(e) = conn.send(&buf[..len]).await {
                log::warn!("{addr}: send error `{e}`, closing connection");
                let _ = conn.close().await;
                break;
            }
            log::trace!("PING >->->->->- {addr}");

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if !ping_response.borrow_mut().remove(&addr) {
            log::warn!("{addr} did not respond, closing connection");
            let _ = conn.close().await;
            return;
        }
    }
}

async fn receive_loop(
    sender: LanMouseSender,
    handle: ClientHandle,
    addr: SocketAddr,
    conn: Arc<dyn Conn + Send + Sync>,
) {
    let LanMouseSender {
        client_manager,
        conns,
        recv_tx: tx,
        ping_response,
        clipboard_peers,
        ..
    } = sender;
    let mut buf = [0u8; MAX_EVENT_SIZE];
    loop {
        // `recv` only overwrites the received datagram, clear leftovers of
        // previous (longer) datagrams so they can not be decoded as payload
        buf.fill(0);
        if conn.recv(&mut buf).await.is_err() {
            break;
        }
        match buf.try_into() {
            Ok(event) => {
                log::trace!("{addr} <==<==<== {event}");
                match event {
                    ProtoEvent::Pong(b) => {
                        client_manager.set_active_addr(handle, Some(addr));
                        client_manager.set_alive(handle, b);
                        ping_response.borrow_mut().insert(addr);
                    }
                    ProtoEvent::Hello { commit, caps } => {
                        client_manager.set_peer_commit(handle, Some(commit));
                        if caps & CAP_CLIPBOARD != 0 {
                            clipboard_peers.borrow_mut().insert(handle);
                        } else {
                            clipboard_peers.borrow_mut().remove(&handle);
                        }
                    }
                    event => tx.send((handle, event)).expect("channel closed"),
                }
            }
            // Skip undecodable datagrams without dropping the
            // connection. Each DTLS recv is one framed message, so
            // skipping is safe and keeps us forward-compatible with
            // peers that send event types we don't yet know about.
            Err(e) => log::debug!("ignoring undecodable event from {addr}: {e}"),
        }
    }
    log::warn!("recv error");
    disconnect(&client_manager, handle, addr, &conns, &clipboard_peers).await;
}

async fn disconnect(
    client_manager: &ClientManager,
    handle: ClientHandle,
    addr: SocketAddr,
    conns: &Mutex<HashMap<SocketAddr, Arc<dyn Conn + Send + Sync>>>,
    clipboard_peers: &RefCell<HashSet<ClientHandle>>,
) {
    log::warn!("client ({handle}) @ {addr} connection closed");
    clipboard_peers.borrow_mut().remove(&handle);
    conns.lock().await.remove(&addr);
    client_manager.set_active_addr(handle, None);
    client_manager.set_peer_commit(handle, None);
    let active: Vec<SocketAddr> = conns.lock().await.keys().copied().collect();
    log::info!("active connections: {active:?}");
}
