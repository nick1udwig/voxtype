//! Bounded, opt-in session resumption. The admission still comes from Daemon;
//! this transport retains audio/state, never a model or an inference factory.
use super::*;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;

const CACHE_SIZE: usize = 8;
const RECONNECT_GRACE: Duration = Duration::from_secs(30);
const RESULT_TTL: Duration = Duration::from_secs(90);
const SESSION_LIMIT: Duration = Duration::from_secs(480);

#[derive(Clone)]
struct Snapshot {
    ready: Value,
    accepted: usize,
    finished: bool,
    epoch: u64,
    attached: bool,
    event: Option<Value>,
    revision: u64,
    terminal: bool,
}
impl Snapshot {
    fn ready_event(&self) -> Value {
        let mut ready = self.ready.clone();
        ready["resume"] = json!(true);
        ready["accepted_bytes"] = json!(self.accepted);
        ready["finished"] = json!(self.finished);
        ready["framing"] = json!("offset_pcm_s16le");
        ready
    }
    fn event(&mut self, event: Value) {
        self.terminal = matches!(event["type"].as_str(), Some("final" | "error"));
        self.event = Some(event);
        self.revision += 1;
    }
}

enum Command {
    Attach(oneshot::Sender<Snapshot>),
    Audio {
        epoch: u64,
        offset: usize,
        bytes: Vec<u8>,
    },
    Finish(u64),
    Detach(u64),
    Cancel(u64),
    // A new dictation can discard an unfinished, disconnected predecessor.
    Abandon,
    Ack(u64),
    Evict,
}
struct Entry {
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<Snapshot>,
}

#[derive(Default)]
pub(super) struct Registry {
    entries: Mutex<HashMap<String, Arc<Entry>>>,
}
impl Registry {
    async fn get(
        &self,
        id: &str,
        requests: &mpsc::Sender<RequestSession>,
    ) -> Result<Arc<Entry>, &'static str> {
        let mut entries = self.entries.lock().await;
        entries.retain(|_, e| !e.commands.is_closed());
        if let Some(entry) = entries.get(id) {
            return Ok(entry.clone());
        }
        for entry in entries.values() {
            let state = entry.state.borrow();
            if !state.attached && !state.finished && !state.terminal {
                let _ = entry.commands.try_send(Command::Abandon);
            }
        }
        if entries.len() >= CACHE_SIZE {
            let old = entries
                .iter()
                .find(|(_, e)| e.state.borrow().terminal)
                .map(|(id, _)| id.clone());
            if let Some(old) = old {
                if let Some(entry) = entries.remove(&old) {
                    let _ = entry.commands.send(Command::Evict).await;
                }
            } else {
                return Err("busy");
            }
        }
        let (reply, answer) = oneshot::channel();
        requests
            .try_send(RequestSession { reply })
            .map_err(|_| "busy")?;
        let mut admission = tokio::time::timeout(Duration::from_secs(10), answer)
            .await
            .map_err(|_| "busy")?
            .map_err(|_| "busy")??;
        let ready = tokio::time::timeout(Duration::from_secs(5), admission.events.recv())
            .await
            .map_err(|_| "model_unavailable")?
            .ok_or("model_unavailable")?;
        if ready["type"] != "ready" {
            return Err("model_unavailable");
        }
        let entry = spawn(admission, ready);
        entries.insert(id.to_owned(), entry.clone());
        Ok(entry)
    }

    pub(super) async fn connect(
        &self,
        id: String,
        requests: mpsc::Sender<RequestSession>,
        mut ws: WebSocketStream<tokio::net::TcpStream>,
    ) {
        let entry = match self.get(&id, &requests).await {
            Ok(e) => e,
            Err(code) => {
                let _ = send(&mut ws, &json!({"type":"error","code":code})).await;
                return;
            }
        };
        let (reply, answer) = oneshot::channel();
        if entry.commands.send(Command::Attach(reply)).await.is_err() {
            return;
        }
        let Ok(snapshot) = answer.await else {
            return;
        };
        let epoch = snapshot.epoch;
        let mut updates = entry.state.clone();
        async {
            if !send(&mut ws, &snapshot.ready_event()).await { return; }
            let mut revision = 0;
            let mut pending = None;
            loop {
                // Reading the latest snapshot also covers results arriving between attach and ready.
                let current = updates.borrow_and_update().clone();
                if current.epoch != epoch { return; }
                if current.revision != revision {
                    revision = current.revision;
                    if let Some(event) = current.event {
                        if !send(&mut ws, &event).await { return; }
                        // Keep the socket open for an acknowledgement or reconnect.
                    }
                }
                tokio::select! {
                    changed = updates.changed() => { if changed.is_err() { return; } }
                    permit = entry.commands.reserve(), if pending.is_some() => {
                        let Ok(permit) = permit else { return; };
                        permit.send(pending.take().unwrap());
                    }
                    message = ws.next(), if pending.is_none() => match message {
                        Some(Ok(Message::Binary(bytes))) => {
                            if bytes.len() < 10 || (bytes.len() - 8) % 2 != 0 { pending = Some(Command::Cancel(epoch)); }
                            else {
                                let raw = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                                match usize::try_from(raw) {
                                    Ok(offset) => pending = Some(Command::Audio { epoch, offset, bytes: bytes[8..].to_vec() }),
                                    Err(_) => pending = Some(Command::Cancel(epoch)),
                                }
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            let kind = serde_json::from_str::<Value>(&text).ok()
                                .and_then(|v| v["type"].as_str().map(str::to_owned));
                            pending = Some(match kind.as_deref() {
                                Some("finish") => Command::Finish(epoch),
                                Some("ack") => Command::Ack(epoch),
                                _ => Command::Cancel(epoch),
                            });
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if !matches!(tokio::time::timeout(Duration::from_secs(5), ws.send(Message::Pong(payload))).await, Ok(Ok(()))) { return; }
                        }
                        Some(Ok(Message::Pong(_))) => {},
                        _ => return,
                    }
                }
            }
        }.await;
        // Queue this after any admitted commands. A stale connection cannot detach its replacement.
        let _ = entry.commands.send(Command::Detach(epoch)).await;
    }
}

async fn send(ws: &mut WebSocketStream<tokio::net::TcpStream>, event: &Value) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            ws.send(Message::Text(event.to_string()))
        )
        .await,
        Ok(Ok(()))
    )
}

fn spawn(admission: Admission, ready: Value) -> Arc<Entry> {
    let snapshot = Snapshot {
        ready,
        accepted: 0,
        finished: false,
        epoch: 0,
        attached: true,
        event: None,
        revision: 0,
        terminal: false,
    };
    let (updates, state) = watch::channel(snapshot.clone());
    let (commands, mut rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let mut snapshot = snapshot;
        let mut audio = Some(admission.audio);
        let mut events = admission.events;
        let mut queue = VecDeque::<Vec<f32>>::new();
        let mut finish_forwarded = false;
        let hard_deadline = Instant::now() + SESSION_LIMIT;
        let started = Instant::now();
        let mut finish_received: Option<Instant> = None;
        let mut expiry = Instant::now() + RECONNECT_GRACE;
        let mut terminal_expiry = None;
        let max_bytes = snapshot.ready["max_seconds"]
            .as_u64()
            .unwrap_or(300)
            .min(300) as usize
            * 32000;
        loop {
            let deadline = terminal_expiry.unwrap_or(if snapshot.attached || snapshot.finished {
                hard_deadline
            } else {
                expiry.min(hard_deadline)
            });
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    if snapshot.terminal { break; }
                    snapshot.event(json!({"type":"error","code":"session_expired"}));
                }
                event = events.recv(), if !snapshot.terminal => {
                    snapshot.event(event.unwrap_or_else(|| json!({"type":"error","code":"transcription_failed"})));
                }
                command = rx.recv() => {
                    let Some(command) = command else { break; };
                    match command {
                        Command::Attach(reply) => {
                            snapshot.epoch += 1;
                            snapshot.attached = true;
                            tracing::info!(resumed = snapshot.epoch > 1, accepted_bytes = snapshot.accepted,
                                finished = snapshot.finished, "Mobile session attached");
                            updates.send_replace(snapshot.clone());
                            let _ = reply.send(snapshot.clone());
                        }
                        Command::Detach(epoch) if epoch == snapshot.epoch => {
                            snapshot.attached = false;
                            expiry = Instant::now() + if snapshot.accepted == 0 { Duration::from_secs(2) } else { RECONNECT_GRACE };
                            updates.send_replace(snapshot.clone());
                        }
                        Command::Audio { epoch, offset, bytes } if epoch == snapshot.epoch && !snapshot.terminal => {
                            let end = offset.checked_add(bytes.len());
                            if end.is_some_and(|end| end <= snapshot.accepted) && offset % 2 == 0
                                && !bytes.is_empty() && bytes.len() % 2 == 0 {
                                continue; // An already accepted frame is a harmless duplicate.
                            }
                            if offset != snapshot.accepted || snapshot.finished || bytes.is_empty() || bytes.len() % 2 != 0
                                || end.is_none_or(|end| end > max_bytes) {
                                snapshot.event(json!({"type":"error","code":"invalid_audio_offset"}));
                            } else {
                                // Aggregate tiny frames so a bounded audio duration also bounds
                                // allocation overhead. At most 3000 100-ms chunks / 19.2 MB PCM.
                                for sample in decode_pcm(&bytes).unwrap() {
                                    if queue.back().is_none_or(|b| b.len() == 1600) { queue.push_back(Vec::with_capacity(1600)); }
                                    queue.back_mut().unwrap().push(sample);
                                }
                                snapshot.accepted = end.unwrap();
                            }
                        }
                        Command::Finish(epoch) if epoch == snapshot.epoch && !snapshot.terminal => {
                            snapshot.finished = true;
                            if finish_received.is_none() { finish_received = Some(Instant::now()); }
                        }
                        Command::Cancel(epoch) if epoch == snapshot.epoch => {
                            snapshot.event(json!({"type":"error","code":"cancelled"}));
                            terminal_expiry = Some(Instant::now());
                        }
                        Command::Abandon if !snapshot.attached && !snapshot.finished && !snapshot.terminal => {
                            snapshot.event(json!({"type":"error","code":"cancelled"}));
                        }
                        Command::Ack(epoch) if epoch == snapshot.epoch && snapshot.terminal => break,
                        Command::Evict => break,
                        _ => {},
                    }
                }
                permit = async { audio.as_ref().unwrap().clone().reserve_owned().await },
                    if !snapshot.terminal && (!queue.is_empty() || (snapshot.finished && !finish_forwarded)) => {
                    match permit {
                        Ok(permit) => {
                            let input = if let Some(chunk) = queue.pop_front() { Input::Audio(chunk) }
                                else { finish_forwarded = true; Input::Finish };
                            permit.send(input);
                        }
                        Err(_) => snapshot.event(json!({"type":"error","code":"transcription_failed"})),
                    }
                }
            }
            if snapshot.terminal {
                // Daemon keeps ownership/admission until the original inference worker joins.
                audio.take();
                events.close();
                queue.clear();
                if terminal_expiry.is_none() {
                    tracing::info!(
                        audio_secs = snapshot.accepted as f64 / 32000.0,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        after_finish_ms = finish_received.map(|t| t.elapsed().as_millis() as u64),
                        "Mobile resumable session completed"
                    );
                    terminal_expiry = Some(Instant::now() + RESULT_TTL);
                }
            }
            // Do not clone growing partial text for each audio packet.
            let revision = updates.borrow().revision;
            if revision != snapshot.revision {
                updates.send_replace(snapshot.clone());
            }
        }
        // Erase cached transcript at expiry even if the registry entry is not pruned until later.
        snapshot.event = None;
        updates.send_replace(snapshot);
    });
    Arc::new(Entry { commands, state })
}

pub(super) fn valid_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn stub() -> (Arc<Entry>, mpsc::Receiver<Input>, mpsc::Sender<Value>) {
        let (audio, input) = mpsc::channel(1);
        let (out, events) = mpsc::channel(8);
        let entry = spawn(
            Admission { audio, events },
            json!({"type":"ready","max_seconds":1}),
        );
        (entry, input, out)
    }
    async fn attach(entry: &Entry) -> Snapshot {
        let (tx, rx) = oneshot::channel();
        entry.commands.send(Command::Attach(tx)).await.unwrap();
        rx.await.unwrap()
    }
    #[tokio::test(start_paused = true)]
    async fn disconnected_session_expires_and_erases_retained_result() {
        let (entry, mut input, _events) = stub();
        let first = attach(&entry).await;
        entry
            .commands
            .send(Command::Detach(first.epoch))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(RECONNECT_GRACE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(entry.state.borrow().terminal);
        assert!(input.recv().await.is_none());
        tokio::time::advance(RESULT_TTL + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(entry.commands.is_closed());
        assert!(entry.state.borrow().event.is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn finished_session_can_complete_offline_and_ack_erases_cache() {
        let (entry, mut input, events) = stub();
        let first = attach(&entry).await;
        entry
            .commands
            .send(Command::Finish(first.epoch))
            .await
            .unwrap();
        assert!(matches!(input.recv().await, Some(Input::Finish)));
        entry
            .commands
            .send(Command::Detach(first.epoch))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(RECONNECT_GRACE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!entry.state.borrow().terminal);
        events
            .send(json!({"type":"final","text":"retained"}))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        let resumed = attach(&entry).await;
        assert!(resumed.finished);
        assert_eq!(resumed.event.unwrap()["text"], "retained");
        entry
            .commands
            .send(Command::Ack(resumed.epoch))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(entry.commands.is_closed());
        assert!(entry.state.borrow().event.is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn audio_limit_and_stale_lease_are_enforced() {
        let (entry, _input, _events) = stub();
        let old = attach(&entry).await;
        let current = attach(&entry).await;
        entry
            .commands
            .send(Command::Cancel(old.epoch))
            .await
            .unwrap();
        entry
            .commands
            .send(Command::Detach(old.epoch))
            .await
            .unwrap();
        let unchanged = attach(&entry).await;
        assert!(unchanged.attached && !unchanged.terminal);
        assert!(unchanged.epoch > current.epoch);
        entry
            .commands
            .send(Command::Audio {
                epoch: unchanged.epoch,
                offset: 0,
                bytes: vec![0; 32002],
            })
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(entry.state.borrow().terminal);
        assert_eq!(entry.state.borrow().accepted, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn duplicate_frames_do_not_advance_checkpoint_twice() {
        let (entry, _input, _events) = stub();
        let first = attach(&entry).await;
        for _ in 0..2 {
            entry
                .commands
                .send(Command::Audio {
                    epoch: first.epoch,
                    offset: 0,
                    bytes: vec![0; 3200],
                })
                .await
                .unwrap();
        }
        let checkpoint = attach(&entry).await;
        assert_eq!(checkpoint.accepted, 3200);
        assert!(!checkpoint.terminal);
    }
    #[test]
    fn ids_are_bounded_and_header_safe() {
        assert!(valid_id("0123456789abcdef0123456789abcdef"));
        for id in [
            "",
            "bad\r\nheader",
            "0123456789abcdef0123456789abcdef00",
            "0123456789abcdef0123456789abcdeg",
        ] {
            assert!(!valid_id(id));
        }
    }
}
