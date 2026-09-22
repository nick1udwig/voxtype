//! Private mobile transport. This module has no model factory. Admission and
//! final text processing belong to Daemon; all calls use its existing Arc.
use crate::transcribe::streaming::{StreamHandle, StreamingEvent};
use crate::transcribe::Transcriber;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, Semaphore},
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        handshake::server::{Request, Response},
        protocol::WebSocketConfig,
        Message,
    },
};

pub enum Input {
    Audio(Vec<f32>),
    Finish,
}
pub struct Admission {
    pub audio: mpsc::Sender<Input>,
    pub events: mpsc::Receiver<Value>,
}
pub struct RequestSession {
    pub reply: oneshot::Sender<Result<Admission, &'static str>>,
}
pub struct Session {
    pub events: mpsc::Sender<Value>,
    // Never abort this task: a blocking inference must finish before admission
    // is reopened, including when the phone disconnects.
    pub task: JoinHandle<Result<Option<String>, String>>,
}

pub async fn listen() -> std::io::Result<Option<mpsc::Receiver<RequestSession>>> {
    let Some(path) = std::env::var_os("VOXTYPE_REMOTE_TOKEN_FILE") else {
        return Ok(None);
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(&path)?.permissions().mode() & 0o077 != 0 {
            return Err(std::io::Error::other(
                "remote token file must be private (chmod 600)",
            ));
        }
    }
    let token = std::fs::read_to_string(path)?.trim().to_owned();
    if token.len() < 32 {
        return Err(std::io::Error::other(
            "remote token must be at least 32 characters",
        ));
    }
    let addr: SocketAddr = std::env::var("VOXTYPE_REMOTE_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8765".into())
        .parse()
        .map_err(std::io::Error::other)?;
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::other(
            "remote API must bind to loopback; expose it through Tailscale Serve",
        ));
    }
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Mobile API listening (existing daemon model)");
    Ok(Some(serve(listener, token)))
}

fn serve(listener: TcpListener, token: String) -> mpsc::Receiver<RequestSession> {
    let (tx, rx) = mpsc::channel::<RequestSession>(4);
    let slots = Arc::new(Semaphore::new(4));
    let auth = Arc::new(format!("Bearer {token}"));
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let Ok(permit) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let tx = tx.clone();
            let auth = auth.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let config = WebSocketConfig {
                    max_message_size: Some(65536),
                    max_frame_size: Some(65536),
                    ..Default::default()
                };
                let handshake = accept_hdr_async_with_config(
                    socket,
                    move |req: &Request, response: Response| {
                        let supplied = req
                            .headers()
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        if req.uri().path() != "/v1/dictate"
                            || req.headers().contains_key("origin")
                            || !equal_secret(supplied.as_bytes(), auth.as_bytes())
                        {
                            return Err(Response::builder()
                                .status(401)
                                .body(Some("Unauthorized".into()))
                                .unwrap());
                        }
                        Ok(response)
                    },
                    Some(config),
                );
                let Ok(Ok(mut ws)) = tokio::time::timeout(Duration::from_secs(5), handshake).await
                else {
                    return;
                };
                let (reply, answer) = oneshot::channel();
                if tx.try_send(RequestSession { reply }).is_err() {
                    return;
                }
                let admission = match tokio::time::timeout(Duration::from_secs(10), answer).await {
                    Ok(Ok(Ok(a))) => a,
                    Ok(Ok(Err(code))) => {
                        let _ = ws
                            .send(Message::Text(
                                json!({"type":"error","code":code}).to_string(),
                            ))
                            .await;
                        return;
                    }
                    _ => return,
                };
                let Admission { audio, mut events } = admission;
                // Dropping audio on every return tells the worker to cancel.
                let deadline = tokio::time::sleep(Duration::from_secs(480));
                tokio::pin!(deadline);
                let mut finished = false;
                loop {
                    tokio::select! {
                        _ = &mut deadline => break,
                        event = events.recv() => {
                            let Some(event) = event else { break };
                            let terminal = event["type"] == "final" || event["type"] == "error";
                            if !matches!(tokio::time::timeout(Duration::from_secs(5), ws.send(Message::Text(event.to_string()))).await, Ok(Ok(()))) { break }
                            if terminal { let _ = ws.close(None).await; break }
                        }
                        message = ws.next() => match message {
                            Some(Ok(Message::Binary(bytes))) if !finished => {
                                let Ok(samples) = decode_pcm(&bytes) else { break };
                                // Bounded queue: overload cancels rather than losing audio.
                                if audio.try_send(Input::Audio(samples)).is_err() { break }
                            }
                            Some(Ok(Message::Text(text))) if !finished => {
                                match serde_json::from_str::<Value>(&text).ok().and_then(|v| v["type"].as_str().map(str::to_owned)).as_deref() {
                                    Some("finish") => { finished = true; if audio.send(Input::Finish).await.is_err() { break } },
                                    _ => break, // includes explicit cancel
                                }
                            }
                            Some(Ok(Message::Ping(p))) => { if ws.send(Message::Pong(p)).await.is_err() { break } },
                            Some(Ok(Message::Pong(_))) => {},
                            _ => break,
                        }
                    }
                }
            });
        }
    });
    rx
}

fn equal_secret(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (a, b)| diff | (a ^ b)) == 0
}
fn decode_pcm(bytes: &[u8]) -> Result<Vec<f32>, ()> {
    if bytes.is_empty() || bytes.len() % 2 != 0 {
        return Err(());
    }
    Ok(bytes
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]) as f32 / 32768.0)
        .collect())
}

pub fn start(
    transcriber: Arc<dyn Transcriber>,
    vad: Option<Arc<dyn crate::vad::VoiceActivityDetector>>,
    max_secs: u32,
) -> (Session, Admission) {
    let (audio, rx) = mpsc::channel(64);
    let (events, event_rx) = mpsc::channel(16);
    let preview = if transcriber.as_streaming().is_some() {
        "streaming"
    } else {
        "batch_snapshot"
    };
    let _ = events.try_send(json!({"type":"ready","protocol":1,"sample_rate":16000,"channels":1,"format":"pcm_s16le","preview":preview,"max_seconds":max_secs}));
    let out = events.clone();
    let task = tokio::spawn(async move {
        if transcriber.as_streaming().is_some() {
            recognize_streaming(transcriber, rx, out, max_secs).await
        } else {
            recognize(transcriber, vad, rx, out, max_secs).await
        }
    });
    (
        Session { events, task },
        Admission {
            audio,
            events: event_rx,
        },
    )
}

async fn recognize_streaming(
    transcriber: Arc<dyn Transcriber>,
    mut input: mpsc::Receiver<Input>,
    events: mpsc::Sender<Value>,
    max_secs: u32,
) -> Result<Option<String>, String> {
    let (audio_tx, audio_rx) = mpsc::channel(64);
    let handle = transcriber
        .as_streaming()
        .unwrap()
        .start_stream(audio_rx)
        .map_err(|e| e.to_string())?;
    let StreamHandle {
        events: mut stream_events,
        cancel,
        task,
    } = handle;
    let mut audio = Some(audio_tx);
    let mut count = 0;
    let mut final_text = None;
    let deadline = tokio::time::sleep(Duration::from_secs(max_secs as u64 + 10));
    tokio::pin!(deadline);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = events.closed() => break Ok(None),
            _ = &mut deadline => break Err("recording_timeout".to_string()),
            chunk = input.recv() => match chunk {
                Some(Input::Audio(chunk)) => {
                    count += chunk.len();
                    if count > max_secs as usize * 16000 { break Err("audio_limit".into()); }
                    let Some(sender) = audio.as_ref() else { break Err("audio_after_finish".into()); };
                    // Never drop audio or grow an unbounded inference backlog.
                    if sender.try_send(chunk).is_err() { break Err("stream_overloaded".into()); }
                }
                Some(Input::Finish) => { audio.take(); }
                None => break Ok(None),
            },
            event = stream_events.recv() => match event {
                Some(StreamingEvent::Partial { text, .. }) => {
                    let _ = events.try_send(json!({"type":"partial", "text":text}));
                }
                Some(StreamingEvent::Final { text, .. }) => {
                    if final_text.replace(text).is_some() { break Err("multiple_stream_finals".into()); }
                }
                Some(StreamingEvent::Ended) if audio.is_none() => {
                    break Ok(Some(final_text.unwrap_or_default()));
                }
                Some(StreamingEvent::Error(error)) => break Err(error.to_string()),
                Some(StreamingEvent::Replace { .. }) => break Err("unsupported_stream_revision".into()),
                _ => break Err("stream_ended_early".into()),
            }
        }
    };
    if !matches!(outcome, Ok(Some(_))) {
        let _ = cancel.send(());
    }
    drop(audio);
    // Close the event consumer to unblock any backend waiting to emit. Join the
    // actual worker before dropping the model Arc or returning admission to Daemon.
    drop(stream_events);
    let joined = task
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r.map_err(|e| e.to_string()));
    if matches!(outcome, Ok(Some(_))) {
        joined?;
    }
    outcome
}

async fn infer(t: Arc<dyn Transcriber>, samples: Vec<f32>) -> Result<String, String> {
    tokio::task::spawn_blocking(move || t.transcribe(&samples))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
}

async fn recognize(
    t: Arc<dyn Transcriber>,
    vad: Option<Arc<dyn crate::vad::VoiceActivityDetector>>,
    mut input: mpsc::Receiver<Input>,
    events: mpsc::Sender<Value>,
    max_secs: u32,
) -> Result<Option<String>, String> {
    let mut samples = Vec::new();
    let mut preview: Option<JoinHandle<Result<String, String>>> = None;
    let mut last_preview = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let deadline = tokio::time::sleep(Duration::from_secs(max_secs as u64 + 10));
    tokio::pin!(deadline);
    let finish = loop {
        tokio::select! {
            _ = &mut deadline => break Err("recording_timeout"),
            chunk = input.recv() => match chunk {
                Some(Input::Audio(audio)) => {
                    if samples.len() + audio.len() > max_secs as usize * 16000 { break Err("audio_limit") }
                    samples.extend(audio);
                }
                Some(Input::Finish) => break Ok(true),
                None => break Ok(false),
            },
            _ = tick.tick() => {
                if preview.as_ref().is_some_and(|p| p.is_finished()) {
                    if let Ok(Ok(text)) = preview.take().unwrap().await {
                        let _ = events.try_send(json!({"type":"partial","text":text}));
                    }
                }
                if preview.is_none() && samples.len() >= last_preview + 32000 {
                    last_preview = samples.len();
                    preview = Some(tokio::spawn(infer(t.clone(), samples.clone())));
                }
            }
        }
    };
    // A dropped socket or cancel cannot free the pipeline while inference runs.
    if let Some(task) = preview {
        let _ = task.await;
    }
    match finish {
        Ok(false) => Ok(None),
        Err(code) => Err(code.into()),
        Ok(true) if samples.len() < 4800 => Ok(Some(String::new())),
        Ok(true) => {
            if let Some(vad) = vad {
                if matches!(vad.detect(&samples), Ok(result) if !result.has_speech) {
                    return Ok(Some(String::new()));
                }
            }
            infer(t, samples).await.map(Some)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcribe::streaming::StreamingTranscriber;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct StreamFake {
        active: Arc<AtomicUsize>,
        starts: Arc<AtomicUsize>,
        cancelled: Arc<AtomicUsize>,
        fail_flush: bool,
    }
    impl StreamFake {
        fn new(fail_flush: bool) -> Self {
            Self {
                active: Arc::new(AtomicUsize::new(0)),
                starts: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                fail_flush,
            }
        }
    }
    impl Transcriber for StreamFake {
        fn transcribe(&self, _: &[f32]) -> Result<String, crate::error::TranscribeError> {
            panic!("streaming must never call batch inference")
        }
        fn as_streaming(&self) -> Option<&dyn StreamingTranscriber> {
            Some(self)
        }
    }
    impl StreamingTranscriber for StreamFake {
        fn start_stream(
            &self,
            mut samples: mpsc::Receiver<Vec<f32>>,
        ) -> Result<StreamHandle, crate::error::TranscribeError> {
            assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
            self.starts.fetch_add(1, Ordering::SeqCst);
            let active = self.active.clone();
            let cancelled = self.cancelled.clone();
            let fail_flush = self.fail_flush;
            let (tx, events) = mpsc::channel(16);
            let (cancel, mut cancel_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut cancel_rx => { cancelled.fetch_add(1, Ordering::SeqCst); break; }
                        chunk = samples.recv() => {
                            if chunk.is_none() {
                                let event = if fail_flush {
                                    StreamingEvent::Error(crate::error::TranscribeError::InferenceFailed("flush failed".into()))
                                } else { StreamingEvent::Final {text:"hello world".into(), segment_id:0} };
                                let _ = tx.send(event).await;
                                let _ = tx.send(StreamingEvent::Ended).await;
                                break;
                            }
                            let _ = tx.send(StreamingEvent::Partial {text:"hello".into(), segment_id:0}).await;
                        }
                    }
                }
                // A backend can finish its event stream before inference resources are released.
                tokio::time::sleep(Duration::from_millis(30)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            });
            Ok(StreamHandle {
                events,
                cancel,
                task,
            })
        }
    }
    #[tokio::test]
    async fn streaming_reuses_model_and_finishes_without_a_batch_pass() {
        let model = Arc::new(StreamFake::new(false));
        for _ in 0..2 {
            let (session, mut a) = start(model.clone(), None, 5);
            assert_eq!(a.events.recv().await.unwrap()["preview"], "streaming");
            a.audio.send(Input::Audio(vec![0.1; 3200])).await.unwrap();
            assert_eq!(a.events.recv().await.unwrap()["text"], "hello");
            a.audio.send(Input::Finish).await.unwrap();
            assert_eq!(
                session.task.await.unwrap().unwrap().as_deref(),
                Some("hello world")
            );
            assert_eq!(model.active.load(Ordering::SeqCst), 0);
            assert_eq!(Arc::strong_count(&model), 1);
        }
        assert_eq!(model.starts.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn streaming_disconnect_joins_worker_before_releasing_model() {
        let model = Arc::new(StreamFake::new(false));
        let (session, mut a) = start(model.clone(), None, 5);
        a.events.recv().await.unwrap();
        a.audio.send(Input::Audio(vec![0.1; 3200])).await.unwrap();
        a.events.recv().await.unwrap();
        drop(a);
        assert!(session.task.await.unwrap().unwrap().is_none());
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        assert_eq!(model.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&model), 1);
    }
    #[tokio::test]
    async fn streaming_errors_do_not_fall_back_to_batch_or_partial_final() {
        let model = Arc::new(StreamFake::new(true));
        let (session, a) = start(model.clone(), None, 5);
        a.audio.send(Input::Finish).await.unwrap();
        assert!(session
            .task
            .await
            .unwrap()
            .unwrap_err()
            .contains("flush failed"));
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn streaming_audio_limit_cancels_and_joins() {
        let model = Arc::new(StreamFake::new(false));
        let (session, a) = start(model.clone(), None, 1);
        a.audio.send(Input::Audio(vec![0.1; 16001])).await.unwrap();
        assert_eq!(session.task.await.unwrap().unwrap_err(), "audio_limit");
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
    }
    struct Fake {
        calls: AtomicUsize,
        active: AtomicUsize,
    }
    impl Transcriber for Fake {
        fn transcribe(&self, _: &[f32]) -> Result<String, crate::error::TranscribeError> {
            assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(30));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok("hello".into())
        }
    }
    #[test]
    fn pcm_and_auth() {
        assert_eq!(
            decode_pcm(&[0, 128, 255, 127]).unwrap(),
            vec![-1.0, 32767.0 / 32768.0]
        );
        assert!(decode_pcm(&[1]).is_err());
        assert!(equal_secret(b"secret", b"secret"));
        assert!(!equal_secret(b"secret", b"secreT"));
    }
    #[tokio::test]
    async fn reuses_model_for_preview_final_and_next_session() {
        let model = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        });
        for _ in 0..2 {
            let (session, mut a) = start(model.clone(), None, 5);
            a.audio.send(Input::Audio(vec![0.1; 32000])).await.unwrap();
            while a.events.recv().await.unwrap()["type"] != "partial" {}
            a.audio.send(Input::Finish).await.unwrap();
            assert_eq!(session.task.await.unwrap().unwrap().unwrap(), "hello");
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 4);
        assert_eq!(Arc::strong_count(&model), 1);
    }
    #[tokio::test]
    async fn disconnect_drains_inference_and_releases_reference() {
        let model = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        });
        let (session, a) = start(model.clone(), None, 5);
        a.audio.send(Input::Audio(vec![0.1; 32000])).await.unwrap();
        while model.active.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(a);
        assert!(session.task.await.unwrap().unwrap().is_none());
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        assert_eq!(Arc::strong_count(&model), 1);
    }
    #[tokio::test]
    async fn audio_limit_is_enforced() {
        let model = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        });
        let (session, a) = start(model, None, 1);
        a.audio.send(Input::Audio(vec![0.0; 16001])).await.unwrap();
        assert_eq!(session.task.await.unwrap().unwrap_err(), "audio_limit");
    }
    #[tokio::test]
    async fn websocket_auth_audio_finish_and_disconnect() {
        use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v1/dictate", listener.local_addr().unwrap());
        let mut requests = serve(listener, "test-secret".into());
        assert!(connect_async(&url).await.is_err());
        assert!(requests.try_recv().is_err());
        let model = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        });
        for finish in [true, false] {
            let mut request = url.clone().into_client_request().unwrap();
            request
                .headers_mut()
                .insert("Authorization", "Bearer test-secret".parse().unwrap());
            let (mut ws, _) = connect_async(request).await.unwrap();
            let request = requests.recv().await.unwrap();
            let (session, admission) = start(model.clone(), None, 5);
            assert!(request.reply.send(Ok(admission)).is_ok());
            let ready = ws.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&ready).unwrap()["type"],
                "ready"
            );
            ws.send(Message::Binary(vec![0; 16000])).await.unwrap();
            if finish {
                ws.send(Message::Text(r#"{"type":"finish"}"#.into()))
                    .await
                    .unwrap();
                let text = session.task.await.unwrap().unwrap().unwrap();
                session
                    .events
                    .send(json!({"type":"final","text":text}))
                    .await
                    .unwrap();
                let final_event = ws.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&final_event).unwrap()["text"],
                    "hello"
                );
            } else {
                ws.close(None).await.unwrap();
                assert!(tokio::time::timeout(Duration::from_secs(2), session.task)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .is_none());
            }
        }
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    }
}
