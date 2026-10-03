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

mod opus;
mod resume;

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
    let resumes = Arc::new(resume::Registry::default());
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            // Small audio and result frames should be forwarded promptly.
            let _ = socket.set_nodelay(true);
            let Ok(permit) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let tx = tx.clone();
            let auth = auth.clone();
            let resumes = resumes.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let config = WebSocketConfig {
                    max_message_size: Some(65536),
                    max_frame_size: Some(65536),
                    ..Default::default()
                };
                let session_id = Arc::new(std::sync::Mutex::new(None::<String>));
                let header_id = session_id.clone();
                let capabilities = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let header_capabilities = capabilities.clone();
                let handshake = accept_hdr_async_with_config(
                    socket,
                    move |req: &Request, response: Response| {
                        let supplied = req
                            .headers()
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        if !matches!(req.uri().path(), "/v1/dictate" | "/v1/capabilities")
                            || req.headers().contains_key("origin")
                            || !equal_secret(supplied.as_bytes(), auth.as_bytes())
                        {
                            return Err(Response::builder()
                                .status(401)
                                .body(Some("Unauthorized".into()))
                                .unwrap());
                        }
                        header_capabilities.store(
                            req.uri().path() == "/v1/capabilities",
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        if req.uri().path() == "/v1/dictate" {
                            if req
                                .headers()
                                .get("x-voxtype-protocol")
                                .and_then(|v| v.to_str().ok())
                                != Some("2")
                                || req
                                    .headers()
                                    .get("x-voxtype-audio")
                                    .and_then(|v| v.to_str().ok())
                                    != Some(opus::ENCODING)
                            {
                                return Err(Response::builder().status(426).body(Some("Mobile protocol 2 with opus_v1 required; upgrade app and daemon together".into())).unwrap());
                            }
                            let id = req
                                .headers()
                                .get("x-voxtype-session")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("");
                            if !resume::valid_id(id) {
                                return Err(Response::builder()
                                    .status(400)
                                    .body(Some("Valid session ID required".into()))
                                    .unwrap());
                            }
                            *header_id.lock().unwrap() = Some(id.to_owned());
                        }
                        Ok(response)
                    },
                    Some(config),
                );
                let Ok(Ok(mut ws)) = tokio::time::timeout(Duration::from_secs(5), handshake).await
                else {
                    return;
                };
                if capabilities.load(std::sync::atomic::Ordering::Relaxed) {
                    // Discovery never requests admission or touches the loaded model.
                    let event = json!({"type":"capabilities", "protocol":2,
                        "dictation":true, "sample_rate":16000, "channels":1,
                        "format":"opus", "results":["partial", "final"],
                        "max_seconds":300, "resume":true,
                        "audio_encodings":[opus::ENCODING], "framing":"sequence_opus_v1", "max_encoded_bytes":opus::MAX_BYTES});
                    let _ = tokio::time::timeout(Duration::from_secs(5), async {
                        ws.send(Message::Text(event.to_string())).await?;
                        ws.close(None).await
                    })
                    .await;
                    return;
                }
                let id = session_id.lock().unwrap().take().unwrap();
                resumes.connect(id, tx, ws).await;
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
    let _ = events.try_send(json!({"type":"ready","protocol":2,"sample_rate":16000,"channels":1,"format":"opus","audio_encoding":opus::ENCODING,"preview":preview,"max_seconds":max_secs}));
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
    let mut pending = None;
    let mut count = 0;
    let mut final_text = None;
    let started = std::time::Instant::now();
    let mut finish_received: Option<std::time::Instant> = None;
    let mut capacity_wait_started = None;
    let mut capacity_wait = Duration::ZERO;
    // Allow catch-up after a slow decoder or a burst of buffered phone audio.
    let deadline = tokio::time::sleep(Duration::from_secs(max_secs as u64 + 180));
    tokio::pin!(deadline);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = events.closed() => break Ok(None),
            _ = &mut deadline => break Err("recording_timeout".to_string()),
            permit = async { audio.as_ref().unwrap().clone().reserve_owned().await }, if pending.is_some() => {
                match permit {
                    Ok(permit) => {
                        if let Some(since) = capacity_wait_started.take() {
                            capacity_wait += std::time::Instant::now() - since;
                        }
                        permit.send(pending.take().unwrap());
                    },
                    Err(_) => break Err("stream_ended_early".into()),
                }
            },
            chunk = input.recv(), if pending.is_none() => match chunk {
                Some(Input::Audio(chunk)) => {
                    count += chunk.len();
                    if count > max_secs as usize * 16000 { break Err("audio_limit".into()); }
                    if audio.is_none() { break Err("audio_after_finish".into()); }
                    // Waiting for capacity must not block consuming backend events:
                    // some backends wait for an event consumer before reading more audio.
                    pending = Some(chunk);
                    capacity_wait_started = Some(std::time::Instant::now());
                }
                Some(Input::Finish) => {
                    finish_received = Some(std::time::Instant::now());
                    audio.take();
                }
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
    tracing::info!(
        audio_secs = count as f64 / 16000.0,
        elapsed_ms = started.elapsed().as_millis() as u64,
        after_finish_ms = finish_received.map(|t| t.elapsed().as_millis() as u64),
        capacity_wait_ms = capacity_wait.as_millis() as u64,
        completed = matches!(outcome, Ok(Some(_))) && joined.is_ok(),
        "Mobile streaming worker completed"
    );
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
        // A cancel/disconnect can close events while we join the preview after
        // Finish. Do not start another inference for an abandoned recording.
        Ok(true) if events.is_closed() => Ok(None),
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
        samples: Arc<AtomicUsize>,
        delay: Duration,
    }
    impl StreamFake {
        fn new(fail_flush: bool) -> Self {
            Self {
                active: Arc::new(AtomicUsize::new(0)),
                starts: Arc::new(AtomicUsize::new(0)),
                cancelled: Arc::new(AtomicUsize::new(0)),
                fail_flush,
                samples: Arc::new(AtomicUsize::new(0)),
                delay: Duration::ZERO,
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
            let received = self.samples.clone();
            let delay = self.delay;
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
                            received.fetch_add(chunk.unwrap().len(), Ordering::SeqCst);
                            tokio::time::sleep(delay).await;
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
    #[tokio::test]
    async fn streaming_burst_backpressures_without_losing_audio_or_deadlocking_events() {
        let mut model = StreamFake::new(false);
        model.delay = Duration::from_millis(2);
        let model = Arc::new(model);
        let (session, a) = start(model.clone(), None, 60);
        let producer = tokio::spawn(async move {
            for _ in 0..300 {
                a.audio.send(Input::Audio(vec![0.1; 1600])).await.unwrap();
            }
            a.audio.send(Input::Finish).await.unwrap();
            a
        });
        let result = tokio::time::timeout(Duration::from_secs(5), session.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _admission = producer.await.unwrap();
        assert_eq!(result.as_deref(), Some("hello world"));
        assert_eq!(model.samples.load(Ordering::SeqCst), 300 * 1600);
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancellation_while_backpressured_still_joins_worker() {
        let mut model = StreamFake::new(false);
        model.delay = Duration::from_millis(20);
        let model = Arc::new(model);
        let (session, a) = start(model.clone(), None, 60);
        let producer = tokio::spawn(async move {
            for _ in 0..300 {
                if a.audio.send(Input::Audio(vec![0.1; 1600])).await.is_err() {
                    break;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(a.events);
        assert!(tokio::time::timeout(Duration::from_secs(2), session.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none());
        producer.await.unwrap();
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        assert_eq!(model.cancelled.load(Ordering::SeqCst), 1);
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
    type TestSocket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;
    async fn resume_socket(url: &str, id: &str) -> TestSocket {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = url.into_client_request().unwrap();
        for (name, value) in [
            ("Authorization", "Bearer resume-test"),
            ("X-Voxtype-Session", id),
            ("X-Voxtype-Protocol", "2"),
            ("X-Voxtype-Audio", opus::ENCODING),
        ] {
            request.headers_mut().insert(name, value.parse().unwrap());
        }
        tokio_tungstenite::connect_async(request).await.unwrap().0
    }
    async fn next_event(ws: &mut TestSocket) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(&message.into_text().unwrap()).unwrap()
    }
    async fn send_frame(
        ws: &mut TestSocket,
        sequence: u64,
        count: usize,
        last: bool,
        total: usize,
    ) {
        ws.send(Message::Binary(opus::tests::frame(
            sequence,
            count,
            sequence == 0,
            last,
            total,
        )))
        .await
        .unwrap();
    }
    async fn resumable_server() -> (String, Arc<StreamFake>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v1/dictate", listener.local_addr().unwrap());
        let mut requests = serve(listener, "resume-test".into());
        let model = Arc::new(StreamFake::new(false));
        let shared = model.clone();
        let supervisor = tokio::spawn(async move {
            let mut active: Option<Session> = None;
            loop {
                tokio::select! {
                    request = requests.recv() => {
                        let Some(request) = request else { break; };
                        if active.is_some() { let _ = request.reply.send(Err("busy")); }
                        else {
                            let (session, admission) = start(shared.clone(), None, 60);
                            active = Some(session);
                            let _ = request.reply.send(Ok(admission));
                        }
                    }
                    result = async { (&mut active.as_mut().unwrap().task).await }, if active.is_some() => {
                        let session = active.take().unwrap();
                        let event = match result {
                            Ok(Ok(Some(text))) => json!({"type":"final","text":text}),
                            _ => json!({"type":"error","code":"transcription_failed"}),
                        };
                        let _ = session.events.send(event).await;
                    }
                }
            }
        });
        (url, model, supervisor)
    }
    async fn final_event(ws: &mut TestSocket) -> Value {
        loop {
            let event = next_event(ws).await;
            if event["type"] != "partial" {
                return event;
            }
        }
    }
    #[tokio::test]
    async fn resume_after_disconnect_continues_same_worker_without_replaying_audio() {
        let (url, model, supervisor) = resumable_server().await;
        let id = "11111111111111111111111111111111";
        let mut ws = resume_socket(&url, id).await;
        assert_eq!(next_event(&mut ws).await["resume"], true);
        send_frame(&mut ws, 0, 5, false, 0).await;
        assert_eq!(next_event(&mut ws).await["type"], "partial");
        drop(ws);
        let mut resumed = resume_socket(&url, id).await;
        assert_eq!(next_event(&mut resumed).await["accepted_frames"], 1);
        send_frame(&mut resumed, 0, 5, false, 0).await; // Duplicate must not reach inference.
        send_frame(&mut resumed, 1, 3, true, 2400).await;
        resumed
            .send(Message::Text(r#"{"type":"finish"}"#.into()))
            .await
            .unwrap();
        assert_eq!(final_event(&mut resumed).await["text"], "hello world");
        assert_eq!(model.samples.load(Ordering::SeqCst), 2400);
        assert_eq!(model.starts.load(Ordering::SeqCst), 1);
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        supervisor.abort();
    }
    #[tokio::test]
    async fn completed_transcript_survives_lost_delivery_without_new_inference() {
        let (url, model, supervisor) = resumable_server().await;
        let id = "22222222222222222222222222222222";
        let mut ws = resume_socket(&url, id).await;
        next_event(&mut ws).await;
        send_frame(&mut ws, 0, 5, false, 0).await;
        send_frame(&mut ws, 1, 0, true, 1600).await;
        ws.send(Message::Text(r#"{"type":"finish"}"#.into()))
            .await
            .unwrap();
        assert_eq!(final_event(&mut ws).await["type"], "final");
        drop(ws); // No result acknowledgement: model result must remain cached.
        let mut resumed = resume_socket(&url, id).await;
        let ready = next_event(&mut resumed).await;
        assert_eq!(ready["finished"], true);
        assert_eq!(ready["accepted_frames"], 2);
        assert_eq!(final_event(&mut resumed).await["text"], "hello world");
        assert_eq!(model.starts.load(Ordering::SeqCst), 1);
        resumed
            .send(Message::Text(r#"{"type":"ack"}"#.into()))
            .await
            .unwrap();
        supervisor.abort();
    }
    #[tokio::test]
    async fn replaced_socket_cannot_cancel_or_append_to_new_attachment() {
        let (url, model, supervisor) = resumable_server().await;
        let id = "33333333333333333333333333333333";
        let mut old = resume_socket(&url, id).await;
        next_event(&mut old).await;
        send_frame(&mut old, 0, 5, false, 0).await;
        next_event(&mut old).await;
        let mut new = resume_socket(&url, id).await;
        assert_eq!(next_event(&mut new).await["accepted_frames"], 1);
        let _ = old.send(Message::Text(r#"{"type":"cancel"}"#.into())).await;
        drop(old);
        send_frame(&mut new, 1, 5, true, 3200).await;
        new.send(Message::Text(r#"{"type":"finish"}"#.into()))
            .await
            .unwrap();
        assert_eq!(final_event(&mut new).await["type"], "final");
        assert_eq!(model.samples.load(Ordering::SeqCst), 3200);
        assert_eq!(model.starts.load(Ordering::SeqCst), 1);
        supervisor.abort();
    }
    #[tokio::test]
    async fn resumed_audio_gap_is_rejected_without_a_partial_success() {
        let (url, model, supervisor) = resumable_server().await;
        let mut ws = resume_socket(&url, "44444444444444444444444444444444").await;
        next_event(&mut ws).await;
        send_frame(&mut ws, 2, 5, false, 0).await;
        assert_eq!(final_event(&mut ws).await["code"], "invalid_audio_sequence");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        assert_eq!(model.samples.load(Ordering::SeqCst), 0);
        supervisor.abort();
    }

    #[tokio::test]
    async fn resumed_cancel_releases_worker_before_next_dictation() {
        let (url, model, supervisor) = resumable_server().await;
        let mut ws = resume_socket(&url, "55555555555555555555555555555555").await;
        next_event(&mut ws).await;
        send_frame(&mut ws, 0, 5, false, 0).await;
        next_event(&mut ws).await;
        ws.send(Message::Text(r#"{"type":"cancel"}"#.into()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while model.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(model.cancelled.load(Ordering::SeqCst), 1);
        let mut next = resume_socket(&url, "66666666666666666666666666666666").await;
        assert_eq!(next_event(&mut next).await["type"], "ready");
        send_frame(&mut next, 0, 0, true, 0).await;
        next.send(Message::Text(r#"{"type":"finish"}"#.into()))
            .await
            .unwrap();
        assert_eq!(final_event(&mut next).await["type"], "final");
        assert_eq!(model.starts.load(Ordering::SeqCst), 2);
        assert_eq!(model.active.load(Ordering::SeqCst), 0);
        supervisor.abort();
    }

    #[test]
    fn authentication() {
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
    async fn cancel_after_finish_joins_preview_without_starting_final_inference() {
        struct GatedFake {
            calls: AtomicUsize,
            started: tokio::sync::Notify,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl Transcriber for GatedFake {
            fn transcribe(&self, _: &[f32]) -> Result<String, crate::error::TranscribeError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.started.notify_one();
                    self.release
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }
                Ok("hello".into())
            }
        }
        let (release, wait) = std::sync::mpsc::channel();
        let model = Arc::new(GatedFake {
            calls: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
            release: std::sync::Mutex::new(wait),
        });
        let (mut session, a) = start(model.clone(), None, 5);
        a.audio.send(Input::Audio(vec![0.1; 32000])).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), model.started.notified())
            .await
            .unwrap();
        a.audio.send(Input::Finish).await.unwrap();
        // Wait until Finish is consumed and recognition is joining the preview.
        tokio::time::timeout(Duration::from_secs(2), async {
            while a.audio.capacity() != a.audio.max_capacity() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(a);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut session.task)
                .await
                .is_err(),
            "cancellation must join the preview before releasing admission"
        );
        release.send(()).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), session.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_none());
        assert_eq!(model.calls.load(Ordering::SeqCst), 1);
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
    async fn capability_discovery_is_authenticated_and_does_not_request_model_admission() {
        use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v1/capabilities", listener.local_addr().unwrap());
        let mut requests = serve(listener, "test-secret".into());
        assert!(connect_async(&url).await.is_err());
        let mut request = url.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Authorization", "Bearer test-secret".parse().unwrap());
        let mut browser = request.clone();
        browser
            .headers_mut()
            .insert("Origin", "https://example.com".parse().unwrap());
        assert!(connect_async(browser).await.is_err());
        let (mut ws, _) = connect_async(request).await.unwrap();
        let event: Value =
            serde_json::from_str(&ws.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
        assert_eq!(event["type"], "capabilities");
        assert_eq!(event["dictation"], true);
        assert_eq!(event["protocol"], 2);
        assert_eq!(event["sample_rate"], 16000);
        assert_eq!(event["format"], "opus");
        assert_eq!(event["results"], json!(["partial", "final"]));
        assert_eq!(event["audio_encodings"], json!([opus::ENCODING]));
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn legacy_clients_are_rejected_before_model_admission() {
        use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/v1/dictate", listener.local_addr().unwrap());
        let mut requests = serve(listener, "test-secret".into());
        for encoding in [None, Some("pcm_s16le"), Some("pcm_s16le_zlib_v1")] {
            let mut request = url.clone().into_client_request().unwrap();
            request
                .headers_mut()
                .insert("Authorization", "Bearer test-secret".parse().unwrap());
            if let Some(value) = encoding {
                request
                    .headers_mut()
                    .insert("X-Voxtype-Audio", value.parse().unwrap());
            }
            let error = connect_async(request).await.unwrap_err();
            assert!(
                matches!(error,tokio_tungstenite::tungstenite::Error::Http(response) if response.status()==426)
            );
        }
        assert!(requests.try_recv().is_err());
    }
    #[tokio::test]
    async fn opus_burst_larger_than_all_audio_queues_finishes_without_loss() {
        let (url, model, supervisor) = resumable_server().await;
        let mut ws = resume_socket(&url, "77777777777777777777777777777777").await;
        next_event(&mut ws).await;
        for sequence in 0..300 {
            send_frame(&mut ws, sequence, 5, false, 0).await;
        }
        send_frame(&mut ws, 300, 0, true, 300 * 1600).await;
        ws.send(Message::Text(r#"{"type":"finish"}"#.into()))
            .await
            .unwrap();
        assert_eq!(final_event(&mut ws).await["type"], "final");
        assert_eq!(model.samples.load(Ordering::SeqCst), 300 * 1600);
        assert_eq!(model.starts.load(Ordering::SeqCst), 1);
        supervisor.abort();
    }
    #[tokio::test]
    async fn malformed_opus_cancels_worker_and_new_session_replays_from_start() {
        let (url, model, supervisor) = resumable_server().await;
        let mut ws = resume_socket(&url, "88888888888888888888888888888888").await;
        next_event(&mut ws).await;
        ws.send(Message::Binary(vec![0; 16])).await.unwrap();
        assert_eq!(final_event(&mut ws).await["code"], "invalid_audio");
        tokio::time::timeout(Duration::from_secs(2), async {
            while model.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(model.samples.load(Ordering::SeqCst), 0);
        let mut next = resume_socket(&url, "99999999999999999999999999999999").await;
        assert_eq!(next_event(&mut next).await["accepted_frames"], 0);
        send_frame(&mut next, 0, 1, true, 1).await;
        next.send(Message::Text(r#"{"type":"finish"}"#.into()))
            .await
            .unwrap();
        assert_eq!(final_event(&mut next).await["type"], "final");
        assert_eq!(model.samples.load(Ordering::SeqCst), 1);
        assert_eq!(model.starts.load(Ordering::SeqCst), 2);
        supervisor.abort();
    }
}
