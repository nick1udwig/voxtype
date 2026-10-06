# Remote dictation fork

The running daemon is the sole model owner. Remote sessions must acquire its
existing transcriber, never construct a model or launch `voxtype transcribe`.
No preview model, inference sidecar or parallel model worker may be introduced.

Admission belongs to the daemon event loop. Unified permits two independent
streams total (two phones, or desktop plus phone) sharing the existing model
handle; its mutex still serializes inference. Batch models remain exclusive.
Cancellation/disconnect must join in-flight inference before that admission slot
can be reused. Audio queues and client recordings stay bounded; use backpressure
without blocking result consumption.
Remote sessions must not type, paste, emit desktop effects or inherit desktop
output context. Final processing still uses the daemon's text/post processors.

Protocol and standalone configuration: `docs/remote-api.md`.
Focused tests (fake models only):

    cargo test --locked --features parakeet --lib remote::
    cargo test --locked --features parakeet --lib parakeet_streaming
    cargo test --locked --features parakeet --lib unified_desktop

Follow CONTRIBUTING.md for upstream review gates; submit to upstream `dev`.
Never start a real model alongside an already-running daemon for a test.
