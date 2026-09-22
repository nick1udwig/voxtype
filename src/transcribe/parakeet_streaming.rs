//! Parakeet streaming transcriber via `parakeet-rs::ParakeetUnified`
//!
//! Wraps the buffered streaming pipeline so voxtype can emit live partial
//! transcripts during recording and a final transcript on hotkey release.
//!
//! Selected when `[parakeet] streaming = true`. Otherwise the batch
//! `ParakeetTranscriber` is used.
//!
//! # Architecture
//!
//! - The transcriber holds a [`ParakeetUnifiedHandle`] (shared model weights)
//!   and a `Mutex<ParakeetUnified>` for batch-mode fallback. The same handle
//!   spawns fresh streaming instances per [`start_stream`] call.
//! - For batch [`Transcriber::transcribe`] the `Mutex<ParakeetUnified>` is
//!   reset between calls so no streaming cache leaks between recordings.
//! - For [`StreamingTranscriber::start_stream`] a fresh `ParakeetUnified` is
//!   constructed from the handle and moved into the spawned drive task.
//!
//! # Partial / Final policy
//!
//! `parakeet-rs` does not surface end-of-utterance signals from the unified
//! pipeline (that lives in the multitalker / EOU model). Until we add VAD-
//! based segmentation, this implementation:
//!
//! - accumulates newly emitted text into cumulative [`StreamingEvent::Partial`]s;
//! - emits one [`StreamingEvent::Final`] with the whole nonempty transcript on
//!   graceful stream close, including the flush tail;
//! - cancellation and errors produce no successful final transcript;
//! - emits exactly one [`StreamingEvent::Ended`] last.
//!
//! This gives us live OSD partials throughout the recording, with the final
//! commit happening at hotkey release. Mid-recording incremental typing
//! (commit-on-pause) is a follow-up once VAD-segmentation lands.

use super::parakeet::{build_execution_config, resolve_model_path};
use super::streaming::{StreamHandle, StreamingEvent, StreamingTranscriber};
use super::{TimedSegment, Transcriber};
use crate::config::ParakeetConfig;
use crate::error::TranscribeError;
use parakeet_rs::{ParakeetUnified, ParakeetUnifiedHandle, UnifiedStreamingConfig};
use std::sync::Mutex;
use tokio::sync::{mpsc, oneshot};

/// Streaming-capable Parakeet transcriber backed by `ParakeetUnified`.
pub struct ParakeetStreamingTranscriber {
    /// Shared model weights. Cloned cheaply for each streaming session.
    handle: ParakeetUnifiedHandle,
    /// Streaming-config snapshot used both for batch (so the handle's
    /// `from_shared_with_streaming_config` succeeds) and for spawning each
    /// streaming task.
    streaming_config: UnifiedStreamingConfig,
    /// Reusable batch instance for `Transcriber::transcribe`. Reset between
    /// calls so no inter-call state leaks.
    batch: Mutex<ParakeetUnified>,
}

impl ParakeetStreamingTranscriber {
    pub fn new(config: &ParakeetConfig) -> Result<Self, TranscribeError> {
        // Streaming-capability gate. The buffered streaming pipeline
        // needs both `tokenizer.model` at load time and a decoder graph
        // sized for the streaming inference loop. Models in the registry
        // that don't meet both (e.g. istupakov's TDT-v3) hit either a
        // tokenizer-not-found at load or an ONNX Gather shape error at
        // first chunk — neither is a useful diagnostic. Fail fast here
        // with a message that names the model and the known-good
        // alternative. Unknown model names (custom user directories)
        // fall through with a warning since we can't validate them.
        if crate::setup::model::is_known_parakeet_model(&config.model)
            && !crate::setup::model::is_streaming_compatible_parakeet(&config.model)
        {
            return Err(TranscribeError::InitFailed(format!(
                "Parakeet streaming is enabled but model `{}` does not support \
                 Parakeet Unified streaming.\n\n\
                 Fix one of:\n\n  \
                 - Disable streaming in config.toml under [parakeet]:\n\n      \
                 streaming = false\n\n  \
                 - Or switch to the streaming-compatible model:\n\n      \
                 voxtype setup model {streaming_model}\n\n    \
                 and set [parakeet] model = \"{streaming_model}\" in config.toml.",
                config.model,
                streaming_model = crate::setup::model::DEFAULT_PARAKEET_STREAMING_MODEL,
            )));
        }
        if !crate::setup::model::is_known_parakeet_model(&config.model) {
            tracing::warn!(
                model = %config.model,
                "Parakeet streaming requested with a model not in the registry; \
                 cannot validate streaming compatibility ahead of time. If load \
                 fails below, switch [parakeet] model to {} or set streaming = false.",
                crate::setup::model::DEFAULT_PARAKEET_STREAMING_MODEL
            );
        }

        // Reject invalid timing before resolving files or loading model weights.
        let streaming_config = streaming_config(config)?;
        let model_path = resolve_model_path(&config.model)?;

        tracing::info!(
            "Loading Parakeet streaming (ParakeetUnified) model from {:?}",
            model_path
        );
        let start = std::time::Instant::now();

        let exec_config = build_execution_config();
        let handle = ParakeetUnifiedHandle::load(&model_path, exec_config).map_err(|e| {
            TranscribeError::InitFailed(format!(
                "Parakeet streaming (ParakeetUnified) init failed: {}\n\n\
                Streaming requires a compatible Parakeet Unified model directory.\n\
                Use parakeet-unified-en-0.6b or set\n\
                [parakeet] streaming = false to use the batch pipeline.",
                e
            ))
        })?;

        let batch = ParakeetUnified::from_shared_with_streaming_config(&handle, streaming_config)
            .map_err(|e| {
            TranscribeError::InitFailed(format!(
                "Failed to spawn batch ParakeetUnified instance: {}",
                e
            ))
        })?;

        tracing::info!(
            "Parakeet streaming model loaded in {:.2}s (chunk={:.2}s, \
            left={:.2}s, right={:.2}s)",
            start.elapsed().as_secs_f32(),
            streaming_config.chunk_secs,
            streaming_config.left_context_secs,
            streaming_config.right_context_secs,
        );

        Ok(Self {
            handle,
            streaming_config,
            batch: Mutex::new(batch),
        })
    }
}

impl Transcriber for ParakeetStreamingTranscriber {
    fn transcribe(&self, samples: &[f32]) -> Result<String, TranscribeError> {
        if samples.is_empty() {
            return Err(TranscribeError::AudioFormat(
                "Empty audio buffer".to_string(),
            ));
        }

        let mut batch = self.batch.lock().map_err(|e| {
            TranscribeError::InferenceFailed(format!("Failed to lock ParakeetUnified mutex: {}", e))
        })?;

        // Reset between batch calls so accumulated state from a prior
        // streaming or batch invocation doesn't bleed in.
        batch.reset();

        let result = batch
            .transcribe_audio(samples.to_vec(), 16000, 1)
            .map_err(|e| {
                TranscribeError::InferenceFailed(format!("ParakeetUnified inference failed: {}", e))
            })?;

        Ok(result.trim().to_string())
    }

    fn transcribe_timed(&self, _samples: &[f32]) -> Result<Vec<TimedSegment>, TranscribeError> {
        // ParakeetUnified does not expose timed segments in the same shape as
        // the batch ParakeetTranscriber. Users who need timed segments should
        // run with [parakeet] streaming = false.
        Err(TranscribeError::InferenceFailed(
            "transcribe_timed is not supported in streaming mode. \
            Set [parakeet] streaming = false for timed segments."
                .to_string(),
        ))
    }

    fn as_streaming(&self) -> Option<&dyn StreamingTranscriber> {
        Some(self)
    }
}

fn streaming_config(config: &ParakeetConfig) -> Result<UnifiedStreamingConfig, TranscribeError> {
    UnifiedStreamingConfig {
        chunk_secs: config.streaming_chunk_secs,
        left_context_secs: config.streaming_left_context_secs,
        right_context_secs: config.streaming_right_context_secs,
    }
    .validate()
    .map_err(|e| {
        TranscribeError::InitFailed(format!(
            "Invalid Parakeet streaming config: {e}. Durations must be multiples of 0.08 seconds. \
         Recommended chunk/left/right: 0.56/5.6/0.56 seconds. Check streaming_chunk_secs, \
         streaming_left_context_secs, streaming_right_context_secs in [parakeet]."
        ))
    })
}

// This adapter lets tests drive the real stream worker without loading any model.
trait ChunkDecoder: Send + 'static {
    fn chunk(&mut self, samples: &[f32]) -> Result<String, TranscribeError>;
    fn flush(&mut self) -> Result<String, TranscribeError>;
}

impl ChunkDecoder for ParakeetUnified {
    fn chunk(&mut self, samples: &[f32]) -> Result<String, TranscribeError> {
        self.transcribe_chunk(samples).map_err(|e| {
            TranscribeError::InferenceFailed(format!(
                "ParakeetUnified::transcribe_chunk failed: {e}"
            ))
        })
    }

    fn flush(&mut self) -> Result<String, TranscribeError> {
        ParakeetUnified::flush(self).map_err(|e| {
            TranscribeError::InferenceFailed(format!("ParakeetUnified::flush failed: {e}"))
        })
    }
}

impl StreamingTranscriber for ParakeetStreamingTranscriber {
    fn start_stream(
        &self,
        samples_rx: mpsc::Receiver<Vec<f32>>,
    ) -> Result<StreamHandle, TranscribeError> {
        // Session state only: the model's ONNX sessions remain shared through self.handle.
        let unified =
            ParakeetUnified::from_shared_with_streaming_config(&self.handle, self.streaming_config)
                .map_err(|e| {
                    TranscribeError::InitFailed(format!(
                        "Failed to spawn streaming ParakeetUnified instance: {e}"
                    ))
                })?;
        Ok(spawn_stream(unified, samples_rx))
    }
}

struct Cancellation {
    receiver: oneshot::Receiver<()>,
    open: bool,
    cancelled: bool,
}

impl Cancellation {
    fn check(&mut self) -> bool {
        if self.open {
            match self.receiver.try_recv() {
                Ok(()) => {
                    self.open = false;
                    self.cancelled = true;
                }
                Err(oneshot::error::TryRecvError::Closed) => self.open = false,
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        }
        self.cancelled
    }

    async fn next(
        &mut self,
        samples: &mut mpsc::Receiver<Vec<f32>>,
        events: &mpsc::Sender<StreamingEvent>,
    ) -> Option<Vec<f32>> {
        if self.check() {
            return None;
        }
        loop {
            tokio::select! {
                biased;
                result = &mut self.receiver, if self.open => {
                    self.open = false;
                    if result.is_ok() { self.cancelled = true; return None; }
                    // Dropping the cancel sender is a no-op per StreamHandle's contract.
                }
                _ = events.closed() => { self.cancelled = true; return None; }
                chunk = samples.recv() => return chunk,
            }
        }
    }
}

fn drive_stream(
    mut decoder: impl ChunkDecoder,
    mut samples: mpsc::Receiver<Vec<f32>>,
    events: &mpsc::Sender<StreamingEvent>,
    cancel: &mut Cancellation,
) -> Result<Option<String>, TranscribeError> {
    let runtime = tokio::runtime::Handle::current();
    let mut text = String::new();
    while let Some(chunk) = runtime.block_on(cancel.next(&mut samples, events)) {
        if chunk.is_empty() {
            continue;
        }
        let delta = decoder.chunk(&chunk);
        // Inference cannot be interrupted. Join it, then discard its result on cancel.
        if cancel.check() || events.is_closed() {
            return Ok(None);
        }
        let delta = delta?;
        if !delta.is_empty() {
            // The library returns new tokens, not snapshots. Preserve spaces, subwords,
            // punctuation and repeated identical deltas exactly as emitted.
            text.push_str(&delta);
            // Previews may be coalesced under backpressure; the final remains complete.
            // Keep two slots for Final/Error and Ended so completion cannot deadlock
            // behind an undrained preview queue. This worker is the only producer.
            if events.capacity() > 2 {
                let _ = events.try_send(StreamingEvent::Partial {
                    text: text.clone(),
                    segment_id: 0,
                });
            }
        }
    }
    if cancel.check() || events.is_closed() {
        return Ok(None);
    }
    let tail = decoder.flush();
    if cancel.check() || events.is_closed() {
        return Ok(None);
    }
    text.push_str(&tail?);
    Ok(Some(text.trim().to_string()))
}

fn spawn_stream(decoder: impl ChunkDecoder, samples: mpsc::Receiver<Vec<f32>>) -> StreamHandle {
    let (events_tx, events_rx) = mpsc::channel::<StreamingEvent>(64);
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let task = tokio::task::spawn_blocking(move || {
        let mut cancel = Cancellation {
            receiver: cancel_rx,
            open: true,
            cancelled: false,
        };
        match drive_stream(decoder, samples, &events_tx, &mut cancel) {
            Ok(Some(text)) if !text.is_empty() => {
                let _ = events_tx.try_send(StreamingEvent::Final {
                    text,
                    segment_id: 0,
                });
            }
            Ok(_) => {}
            Err(error) => {
                let _ = events_tx.try_send(StreamingEvent::Error(error));
            }
        }
        let _ = events_tx.try_send(StreamingEvent::Ended);
    });
    let task = tokio::spawn(async move {
        task.await.map_err(|e| {
            TranscribeError::InferenceFailed(format!("Parakeet streaming task panicked: {e}"))
        })
    });
    StreamHandle {
        events: events_rx,
        cancel: cancel_tx,
        task,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    struct FakeDecoder {
        deltas: VecDeque<Result<String, TranscribeError>>,
        tail: Option<Result<String, TranscribeError>>,
        flushed: Arc<AtomicUsize>,
    }
    impl ChunkDecoder for FakeDecoder {
        fn chunk(&mut self, _: &[f32]) -> Result<String, TranscribeError> {
            self.deltas.pop_front().expect("unexpected inference")
        }
        fn flush(&mut self) -> Result<String, TranscribeError> {
            self.flushed.fetch_add(1, Ordering::SeqCst);
            self.tail.take().expect("duplicate flush")
        }
    }
    fn fake(deltas: &[&str], tail: Result<String, TranscribeError>) -> FakeDecoder {
        FakeDecoder {
            deltas: deltas.iter().map(|s| Ok(s.to_string())).collect(),
            tail: Some(tail),
            flushed: Arc::new(AtomicUsize::new(0)),
        }
    }
    async fn collect(mut handle: StreamHandle) -> Vec<StreamingEvent> {
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut events = Vec::new();
            while let Some(event) = handle.events.recv().await {
                events.push(event);
            }
            handle.task.await.unwrap().unwrap();
            events
        })
        .await
        .expect("stream did not terminate")
    }
    async fn run(deltas: &[&str], tail: Result<String, TranscribeError>) -> Vec<StreamingEvent> {
        let (tx, rx) = mpsc::channel(128);
        let handle = spawn_stream(fake(deltas, tail), rx);
        for _ in deltas {
            tx.send(vec![0.1]).await.unwrap();
        }
        drop(tx);
        collect(handle).await
    }
    fn finals(events: &[StreamingEvent]) -> Vec<&str> {
        events
            .iter()
            .filter_map(|e| match e {
                StreamingEvent::Final { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn application_and_toml_defaults_pass_library_validation() {
        let from_toml: ParakeetConfig =
            toml::from_str("model = 'parakeet-unified-en-0.6b'").unwrap();
        for config in [ParakeetConfig::default(), from_toml] {
            let parsed = streaming_config(&config).unwrap();
            assert_eq!(
                parsed.chunk_secs,
                UnifiedStreamingConfig::default().chunk_secs
            );
            assert_eq!(
                parsed.left_context_secs,
                UnifiedStreamingConfig::default().left_context_secs
            );
            assert_eq!(
                parsed.right_context_secs,
                UnifiedStreamingConfig::default().right_context_secs
            );
        }
    }
    #[test]
    fn invalid_timings_fail_before_resolving_or_loading_model() {
        let config = ParakeetConfig {
            model: "/nonexistent/no-model-may-load".into(),
            streaming_chunk_secs: 0.5,
            ..ParakeetConfig::default()
        };
        let error = match ParakeetStreamingTranscriber::new(&config) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("invalid timing was accepted"),
        };
        assert!(
            error.contains("Invalid Parakeet streaming config"),
            "{error}"
        );
    }
    #[tokio::test]
    async fn deltas_preserve_repeated_words_subwords_spaces_and_flush_tail() {
        let events = run(
            &[" Hel", "lo", " hello", " hello", "", " world"],
            Ok("! ".into()),
        )
        .await;
        let partials: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                StreamingEvent::Partial { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            partials,
            [
                " Hel",
                " Hello",
                " Hello hello",
                " Hello hello hello",
                " Hello hello hello world"
            ]
        );
        assert_eq!(finals(&events), ["Hello hello hello world!"]);
        assert!(matches!(events.last(), Some(StreamingEvent::Ended)));
    }
    #[tokio::test]
    async fn empty_flush_still_returns_all_previous_words() {
        let events = run(&["hello", " world"], Ok(String::new())).await;
        assert_eq!(finals(&events), ["hello world"]);
    }
    #[tokio::test]
    async fn flush_only_short_recording_and_empty_silence() {
        assert_eq!(finals(&run(&[], Ok("short".into())).await), ["short"]);
        assert!(finals(&run(&[""], Ok(String::new())).await).is_empty());
    }
    #[tokio::test]
    async fn flush_failure_is_error_and_never_a_truncated_final() {
        let events = run(
            &["hello"],
            Err(TranscribeError::InferenceFailed("flush failed".into())),
        )
        .await;
        assert!(finals(&events).is_empty());
        assert!(events.iter().any(|e| matches!(e, StreamingEvent::Error(_))));
        assert!(matches!(events.last(), Some(StreamingEvent::Ended)));
    }
    #[tokio::test]
    async fn inference_failure_does_not_flush_or_finalize() {
        let (tx, rx) = mpsc::channel(2);
        let mut decoder = fake(&[], Ok("should not flush".into()));
        decoder
            .deltas
            .push_back(Err(TranscribeError::InferenceFailed("chunk failed".into())));
        let flushed = decoder.flushed.clone();
        let handle = spawn_stream(decoder, rx);
        tx.send(vec![0.1]).await.unwrap();
        drop(tx);
        let events = collect(handle).await;
        assert!(finals(&events).is_empty());
        assert!(events.iter().any(|e| matches!(e, StreamingEvent::Error(_))));
        assert_eq!(flushed.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn cancel_while_waiting_for_audio_exits_without_flush() {
        let (_tx, rx) = mpsc::channel(2);
        let decoder = fake(&[], Ok("must not commit".into()));
        let flushed = decoder.flushed.clone();
        let StreamHandle {
            mut events,
            cancel,
            task,
        } = spawn_stream(decoder, rx);
        cancel.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(events.recv().await, Some(StreamingEvent::Ended)));
        assert!(events.recv().await.is_none());
        assert_eq!(flushed.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn dropped_cancel_sender_is_not_cancellation() {
        let (tx, rx) = mpsc::channel(2);
        let StreamHandle {
            mut events,
            cancel,
            task,
        } = spawn_stream(fake(&["hello"], Ok("!".into())), rx);
        drop(cancel);
        tx.send(vec![0.1]).await.unwrap();
        drop(tx);
        let mut final_text = None;
        while let Some(event) = events.recv().await {
            if let StreamingEvent::Final { text, .. } = event {
                final_text = Some(text);
            }
        }
        task.await.unwrap().unwrap();
        assert_eq!(final_text.as_deref(), Some("hello!"));
    }
    #[tokio::test]
    async fn full_preview_queue_cannot_block_complete_final_and_end() {
        let (tx, rx) = mpsc::channel(128);
        let deltas = vec![" a"; 100];
        let mut handle = spawn_stream(fake(&deltas, Ok("!".into())), rx);
        for _ in &deltas {
            tx.send(vec![0.1]).await.unwrap();
        }
        drop(tx);
        tokio::time::timeout(Duration::from_secs(3), &mut handle.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut events = Vec::new();
        while let Some(event) = handle.events.recv().await {
            events.push(event);
        }
        assert_eq!(finals(&events), [format!("{}!", " a".repeat(100).trim())]);
        assert!(matches!(events.last(), Some(StreamingEvent::Ended)));
    }

    /// Smoke test that the type and trait signatures compile. Real model-
    /// driven tests require ONNX runtime + a downloaded model and live in
    /// the integration test suite gated behind `--features parakeet`.
    #[test]
    fn streaming_config_validation_rejects_zero_chunk() {
        let cfg = UnifiedStreamingConfig {
            chunk_secs: 0.0,
            left_context_secs: 1.0,
            right_context_secs: 0.5,
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn streaming_config_validation_accepts_defaults() {
        let cfg = UnifiedStreamingConfig::default();
        assert!(cfg.validate().is_ok());
    }

    /// Regression for #442: enabling streaming on a known non-streaming model
    /// (e.g. `parakeet-tdt-0.6b-v3`) must fail early in `new()` with a
    /// message that names the model and points the user at the
    /// streaming-compatible alternative, instead of falling through to an
    /// opaque ONNX Gather shape error at the first audio chunk.
    #[test]
    fn new_rejects_streaming_on_known_incompatible_model() {
        let cfg = ParakeetConfig {
            model: "parakeet-tdt-0.6b-v3".to_string(),
            streaming: true,
            ..ParakeetConfig::default()
        };
        // Can't use `.expect_err`: ParakeetStreamingTranscriber wraps
        // parakeet-rs types that don't derive Debug.
        let err = match ParakeetStreamingTranscriber::new(&cfg) {
            Ok(_) => panic!("streaming on a non-streaming model should error before model load"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("parakeet-tdt-0.6b-v3"),
            "error must name the configured model: {msg}"
        );
        assert!(
            msg.contains(crate::setup::model::DEFAULT_PARAKEET_STREAMING_MODEL),
            "error must name the streaming-compatible alternative: {msg}"
        );
        assert!(
            msg.contains("streaming = false") || msg.contains("voxtype setup model"),
            "error must offer at least one concrete fix: {msg}"
        );
    }
}
