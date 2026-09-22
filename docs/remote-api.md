# Remote dictation API (protocol v1)

This optional listener runs inside the existing Voxtype daemon. It accepts
external audio through the daemon-owned transcriber and never loads a second
model. Enable it by setting `VOXTYPE_REMOTE_TOKEN_FILE` to a private token file
in the daemon process environment. `VOXTYPE_REMOTE_BIND` defaults to
`127.0.0.1:8765`; only loopback addresses are accepted. Restart the existing
daemon to enable it. Do not run a second daemon alongside it.

The direct endpoint is `ws://127.0.0.1:8765/v1/dictate`. Expose it only through
a trusted authenticated private-network TLS proxy, such as Tailscale Serve.
The following example uses a `/voxtype` proxy mount. Android is one possible
client; the daemon and protocol build independently of any client repository.

`wss://HOST/voxtype/v1/dictate`, HTTP `Authorization: Bearer TOKEN`.
TLS terminates at Tailscale Serve on port 443. It strips the `/voxtype` mount
prefix and proxies `/v1/dictate` to `127.0.0.1:8765`.
No Funnel, public port, browser origins, query-string credentials or model overrides.
The listener exists only when `VOXTYPE_REMOTE_TOKEN_FILE` is set. Token files must
have mode 0600; use a generated 32-byte random secret. The app encrypts its copy
with Android Keystore and excludes app data from backup.

One WebSocket is one session. Opening it requests admission from the daemon's
main event loop. A busy daemon responds `{"type":"error","code":"busy"}`.
Only four authenticated/handshaking connections may be outstanding.

1. Server: `{"type":"ready","protocol":1,"sample_rate":16000,"channels":1,"format":"pcm_s16le","preview":"streaming","max_seconds":300}`.
   Batch models advertise `"preview":"batch_snapshot"`. Existing Android clients support both.
2. Client: binary frames of signed 16-bit little-endian, mono, 16 kHz PCM.
   The Android client captures immediately on button-down and retains at most
   300 seconds of audio until completion or discard; it sends 100 ms frames. No WAV header or encoding negotiation.
3. Server: `{"type":"partial","text":"cumulative revisable text"}`.
   Every partial replaces the whole preview. It is never inserted into the app.
4. Client: `{"type":"finish"}` after the last audio frame.
5. Server: `{"type":"final","text":"processed complete transcript"}`; closes.
   Empty final means no speech/too short. An error is terminal.

Closing, disconnecting, or sending `{"type":"cancel"}` cancels the session.
Clients may reconnect and replay the complete recording after a transient failure.
A reconnect is a new session; retry `busy` with backoff until the prior worker
has joined and released admission. The daemon never inserts text for a remote
session. Clients must ignore events from abandoned connections and deliver at
most one final result per user dictation. Partial text must never be used as a
successful fallback result. No session resumption or result cache is provided.
Frames are limited to 64 KiB; queues and audio duration are bounded (at most
300 seconds). Server holds admission until all inference has returned, even on
cancellation. Inference cannot be forcibly interrupted safely; a hung backend
keeps the daemon busy until its service is restarted.

Parakeet Unified uses the daemon's existing StreamingTranscriber and shared model
handle. Input goes to a bounded audio channel; cumulative partials are forwarded
and release closes audio so the backend flushes the tail. No batch re-transcription
occurs. Cancel/error/disconnect joins the worker before releasing admission.
Full queues apply backpressure instead of rejecting a temporary backlog.
Both transport and recognition continue consuming result events while waiting
for audio capacity, avoiding circular waits. Each stage holds at most one pending
frame in addition to its bounded 64-frame queue. Streaming allows up to 180
seconds of catch-up beyond the recording limit; the socket session caps at 480 seconds. Streaming does not run the
batch VAD pass; the installed configuration has VAD disabled.

The optional batch Parakeet path produces cumulative previews by periodically
transcribing an audio snapshot through the same Arc used for desktop inference.
At most one inference runs for this remote session. Release waits for an active
preview then transcribes all the audio for the final result. This increases CPU
work and final latency for long recordings; it is not native token streaming.
The configured VAD, text replacement/punctuation processor and post-processing
command are reused. Desktop context, CLI output overrides, typing, clipboard,
notifications, hooks and auto-submit are not invoked for phone sessions.

Streaming backends other than Parakeet Unified, on-demand loading, GPU isolation,
eager processing, meeting mode and secondary models are rejected. No fallback
model loads. Desktop Parakeet sessions also commit only the completed transcript,
so cumulative previews never cause duplicate typing and hold-to-talk remains usable.
