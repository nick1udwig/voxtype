//! Bounded, mandatory session resumption. The admission still comes from Daemon;
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
        ready["accepted_frames"] = json!(self.accepted);
        ready["finished"] = json!(self.finished);
        ready["framing"] = json!("sequence_opus_v1");
        ready["max_encoded_bytes"] = json!(opus::MAX_BYTES);
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
    Audio { epoch: u64, bytes: Vec<u8> },
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
        let decoder = opus::Decoder::new()?;
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
        let entry = spawn(admission, ready, decoder);
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
                            // Parsing and native decoder mutation belong to the session
                            // actor, after socket epoch and frame checkpoint validation.
                            pending = Some(Command::Audio { epoch, bytes });
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

fn spawn(admission: Admission, ready: Value, mut decoder: opus::Decoder) -> Arc<Entry> {
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
        let mut encoded_bytes = 0usize;
        let max_samples = snapshot.ready["max_seconds"]
            .as_u64()
            .unwrap_or(300)
            .min(300) as usize
            * 16000;
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
                            tracing::info!(resumed = snapshot.epoch > 1, accepted_frames = snapshot.accepted,
                                finished = snapshot.finished, "Mobile session attached");
                            updates.send_replace(snapshot.clone());
                            let _ = reply.send(snapshot.clone());
                        }
                        Command::Detach(epoch) if epoch == snapshot.epoch => {
                            snapshot.attached = false;
                            expiry = Instant::now() + if snapshot.accepted == 0 { Duration::from_secs(2) } else { RECONNECT_GRACE };
                            updates.send_replace(snapshot.clone());
                        }
                        Command::Audio { epoch, bytes } if epoch == snapshot.epoch && !snapshot.terminal => {
                            // A duplicate must never touch the stateful native decoder.
                            match opus::Frame::parse(&bytes) {
                                Ok(frame) if frame.sequence < snapshot.accepted => continue,
                                Ok(frame) if frame.sequence == snapshot.accepted && !snapshot.finished
                                    && encoded_bytes + bytes.len() <= opus::MAX_BYTES => {
                                    match decoder.decode(&frame, max_samples) {
                                        Ok(samples) => {
                                            for sample in samples {
                                                if queue.back().is_none_or(|b| b.len() == 1600) { queue.push_back(Vec::with_capacity(1600)); }
                                                queue.back_mut().unwrap().push(sample);
                                            }
                                            encoded_bytes += bytes.len();
                                            snapshot.accepted += 1;
                                        }
                                        Err(_) => snapshot.event(json!({"type":"error","code":"invalid_audio"})),
                                    }
                                }
                                Ok(_) => snapshot.event(json!({"type":"error","code":"invalid_audio_sequence"})),
                                Err(_) => snapshot.event(json!({"type":"error","code":"invalid_audio"})),
                            }
                        }
                        Command::Finish(epoch) if epoch == snapshot.epoch && !snapshot.terminal => {
                            if !decoder.finished {
                                snapshot.event(json!({"type":"error","code":"incomplete_audio"}));
                            } else {
                                snapshot.finished = true;
                                if finish_received.is_none() { finish_received = Some(Instant::now()); }
                            }
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
                        audio_secs = decoder.delivered as f64 / 16000.0,
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
        (
            spawn(
                Admission { audio, events },
                json!({"type":"ready","max_seconds":1}),
                opus::Decoder::new().unwrap(),
            ),
            input,
            out,
        )
    }
    async fn attach(entry: &Entry) -> Snapshot {
        let (tx, rx) = oneshot::channel();
        entry.commands.send(Command::Attach(tx)).await.unwrap();
        rx.await.unwrap()
    }
    async fn audio(
        entry: &Entry,
        epoch: u64,
        sequence: u64,
        count: usize,
        last: bool,
        total: usize,
    ) {
        entry
            .commands
            .send(Command::Audio {
                epoch,
                bytes: opus::tests::frame(sequence, count, sequence == 0, last, total),
            })
            .await
            .unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn disconnected_session_expires_and_erases_cached_error() {
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
    async fn completed_offline_session_replays_result_and_ack_erases_cache() {
        let (entry, mut input, events) = stub();
        let first = attach(&entry).await;
        audio(&entry, first.epoch, 0, 0, true, 0).await;
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
    #[tokio::test]
    async fn stale_socket_and_duplicate_cannot_mutate_decoder_or_checkpoint() {
        let (entry, mut input, _events) = stub();
        let old = attach(&entry).await;
        audio(&entry, old.epoch, 0, 5, false, 0).await;
        let current = attach(&entry).await;
        assert_eq!(current.accepted, 1);
        assert!(matches!(input.recv().await,Some(Input::Audio(pcm)) if pcm.len()==1280));
        audio(&entry, old.epoch, 1, 5, false, 0).await;
        entry
            .commands
            .send(Command::Cancel(old.epoch))
            .await
            .unwrap();
        audio(&entry, current.epoch, 0, 5, false, 0).await;
        audio(&entry, current.epoch, 1, 0, true, 1500).await;
        entry
            .commands
            .send(Command::Finish(current.epoch))
            .await
            .unwrap();
        assert!(matches!(input.recv().await,Some(Input::Audio(pcm)) if pcm.len()==220));
        assert!(matches!(input.recv().await, Some(Input::Finish)));
        assert_eq!(attach(&entry).await.accepted, 2);
    }
    #[tokio::test]
    async fn finish_requires_eos_and_duration_limit_is_enforced() {
        let (entry, _input, _events) = stub();
        let state = attach(&entry).await;
        entry
            .commands
            .send(Command::Finish(state.epoch))
            .await
            .unwrap();
        let state = attach(&entry).await;
        assert_eq!(state.event.unwrap()["code"], "incomplete_audio");
        let (entry, _input, _events) = stub();
        let state = attach(&entry).await;
        audio(&entry, state.epoch, 0, 1, true, 16001).await;
        let state = attach(&entry).await;
        assert!(state.terminal);
        assert_eq!(state.accepted, 0);
    }
    #[tokio::test]
    async fn malformed_later_packet_does_not_admit_partial_frame() {
        let (entry, mut input, _events) = stub();
        let state = attach(&entry).await;
        let mut bytes = opus::tests::frame(0, 2, true, true, 640);
        bytes[24] = 0xff;
        entry
            .commands
            .send(Command::Audio {
                epoch: state.epoch,
                bytes,
            })
            .await
            .unwrap();
        let state = attach(&entry).await;
        assert!(state.terminal);
        assert_eq!(state.accepted, 0);
        assert!(input.recv().await.is_none());
    }
    #[tokio::test]
    async fn bounded_spool_backpressure_still_consumes_cancellation() {
        let (entry, _input, _events) = stub();
        let state = attach(&entry).await;
        for sequence in 0..10 {
            audio(&entry, state.epoch, sequence, 5, false, 0).await;
        }
        entry
            .commands
            .send(Command::Cancel(state.epoch))
            .await
            .unwrap();
        let state = attach(&entry).await;
        assert!(state.terminal);
    }
    #[tokio::test]
    async fn real_opus_decoder_continuity_survives_reconnect_duplicates_and_stale_frames() {
        let golden = opus::tests::golden();
        let mut decoder = opus::Decoder::new().unwrap();
        let mut expected = Vec::new();
        for bytes in &golden {
            expected.extend(
                decoder
                    .decode(&opus::Frame::parse(bytes).unwrap(), 16000)
                    .unwrap(),
            );
        }
        let (entry, mut input, _events) = stub();
        let old = attach(&entry).await;
        entry
            .commands
            .send(Command::Audio {
                epoch: old.epoch,
                bytes: golden[0].clone(),
            })
            .await
            .unwrap();
        let current = attach(&entry).await;
        assert_eq!(current.accepted, 1);
        entry
            .commands
            .send(Command::Audio {
                epoch: old.epoch,
                bytes: golden[1].clone(),
            })
            .await
            .unwrap();
        let mut duplicate = golden[0].clone();
        duplicate[19] = 0xff; // would corrupt native state if decoded
        entry
            .commands
            .send(Command::Audio {
                epoch: current.epoch,
                bytes: duplicate,
            })
            .await
            .unwrap();
        entry
            .commands
            .send(Command::Audio {
                epoch: current.epoch,
                bytes: golden[1].clone(),
            })
            .await
            .unwrap();
        entry
            .commands
            .send(Command::Finish(current.epoch))
            .await
            .unwrap();
        let mut actual = Vec::new();
        while let Some(input) = input.recv().await {
            match input {
                Input::Audio(pcm) => actual.extend(pcm),
                Input::Finish => break,
            }
        }
        assert_eq!(actual, expected);
    }
}
