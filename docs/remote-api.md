# Remote dictation API (protocol 2, Opus only)

The optional listener runs inside the existing Voxtype daemon and uses its
already-loaded transcriber. It never creates a model or inference sidecar.
Enable `VOXTYPE_REMOTE_TOKEN_FILE` with a mode 0600 generated token file;
`VOXTYPE_REMOTE_BIND` defaults to `127.0.0.1:8765` and must be loopback.
Replace/restart the existing daemon for upgrades; never run another alongside it.

The route remains `ws://127.0.0.1:8765/v1/dictate` for proxy compatibility.
Expose it through a trusted private-network TLS proxy, for example Tailscale
Serve at `wss://HOST/voxtype/v1/dictate` (stripping `/voxtype`). No public
Funnel, Origin header, query credentials or model overrides are accepted.

## Admission and versioning

Every dictation handshake requires these headers:

```text
Authorization: Bearer TOKEN
X-Voxtype-Protocol: 2
X-Voxtype-Audio: opus_v1
X-Voxtype-Session: 32 hexadecimal characters
```

Protocol 1 raw PCM/zlib and nonresumable sessions have been removed. Missing or
unsupported protocol/audio headers return HTTP 426 before model admission;
a missing/invalid session ID returns HTTP 400. Android 0.1.9 and this daemon
must be upgraded together. Clients must reject incompatible ready events and
retain saved audio for manual recovery, with no PCM fallback or automatic
version-error retry loop. Four authenticated/handshaking sockets may be active.
The daemon dynamically loads the system Opus library (`libopus.so.0` on Linux,
`libopus.0.dylib` on macOS). Install the runtime package (`opus` on Arch,
`libopus0` on Debian/Ubuntu). Missing codec/API returns `codec_unavailable`
before requesting model admission; restart after installing the codec.

A busy daemon returns `{"type":"error","code":"busy"}`. An admitted or
resumed session first receives:

```json
{"type":"ready","protocol":2,"sample_rate":16000,"channels":1,"format":"opus","audio_encoding":"opus_v1","preview":"streaming","max_seconds":300,"resume":true,"accepted_frames":0,"finished":false,"framing":"sequence_opus_v1","max_encoded_bytes":2097152}
```

Batch models use `preview: batch_snapshot`. Partials remain cumulative revisable
text and are never successful fallback finals. Only one processed `final`
may be delivered to the editor for a user dictation; empty text means no speech.

## Binary audio frames

Opus packets represent mono 20 ms audio; inference receives decoded 16 kHz floats.
No PCM, zlib, WAV/Ogg container or codec-config buffers appear on the wire.
A binary WebSocket message contains exactly one complete frame:

| Bytes | Meaning |
| --- | --- |
| 8 | Unsigned big-endian frame sequence, beginning at 0 |
| 1 | Envelope version 1 |
| 1 | Flags: first=1, final=2; other bits forbidden |
| 2 | Unsigned big-endian pre-skip in 48 kHz samples, first frame only |
| 4 | Unsigned big-endian original 16 kHz source sample count, final frame only |
| 1 | Packet count 0..5; zero permitted only on final frame |
| Repeated | Unsigned big-endian u16 packet length, then packet bytes |

Each packet is 1..1275 bytes, mono and exactly 20 ms. Frame size is 17..6402 bytes.
The first flag is required exactly on sequence 0. Pre-skip must be divisible by 3
and at most 3840 (80 ms); the decoded sample rate remains 16 kHz. Nonfirst pre-skip
and nonfinal original count must be 0. Empty input may be a first + final frame
with 0 packets, 0 pre-skip and 0 original samples. A final count must be restorable
from actual decoded samples; it cannot invent audio. Trailing bytes, truncation,
stereo, unsupported packet durations, overflows and changed boundaries fail.

Android captures 16 kHz signed 16-bit microphone PCM and encodes during capture
through platform MediaCodec `audio/opus`, target 24 kbit/s (Android 10+).
Codec config is parsed rather than sent: `csd-1` native-order unsigned 64-bit
nanosecond pre-skip overrides OpusHead, including unified AOPUSHDR/AOPUSDLY
headers. The app pads with bounded zeros to cover the actual encoder delay,
queues EOS and drains delayed output (each wait has a 5 second deadline), then
marks the last retained packet aggregate final. Empty EOS is supported.
The daemon removes pre-skip and trims the tail to exact original sample count;
it retains one 20 ms decoded tail until final so empty EOS cannot require retracting
model input. Source duration is at most 300 seconds, encoded recording at most
2 MiB and 15002 frames. Decoded padding is bounded separately. Android retains
encoded frames only. A bounded eight-chunk PCM handoff (100 ms per chunk)
lets microphone capture begin before codec initialization and preserves a quick
release during startup. One producer chunk and at most 5 pending encoded packets
are retained in addition to this queue. Saturation during live capture fails
explicitly without blocking microphone reads or returning truncated success;
only the final marker may wait after capture stops. The app never re-encodes
during reconnect. Opus is lossy.

Platform codec application/VBR choices are device controlled. AOSP C2 uses
APPLICATION_AUDIO and constrained VBR; earlier host benchmarks used VOIP and
unconstrained VBR. Those timing/size numbers do not measure the phone encoder.
See [MediaCodec config/EOS documentation](https://developer.android.com/reference/android/media/MediaCodec)
and [AOSP Opus encoder](https://android.googlesource.com/platform/frameworks/av/+/1401a72389/media/codec2/components/opus/C2SoftOpusEnc.cpp).

After the final frame, send `{"type":"finish"}`. Finish without an accepted
final audio boundary fails with `incomplete_audio`. The daemon flushes the
existing recognizer and emits `{"type":"final","text":"processed text"}`.
Send `{"type":"ack"}` after accepting it, then close. Explicit cancel is
`{"type":"cancel"}` and never produces editor output.

## Resume, expiry and ownership

Each logical session actor owns its stateful Opus decoder, independent of
sockets. Socket epoch and sequence checks run before native decoder mutation.
A complete accepted frame advances `accepted_frames` by 1; duplicates with
preceding sequence are ignored before decoding. Gaps fail with
`invalid_audio_sequence`; invalid frames fail with `invalid_audio`. No partial
frame reaches inference or advances the checkpoint. Reconnecting with the same
ID retains codec state and the worker or cached final. The old socket loses its
lease and cannot append, cancel or detach its replacement.

Resume from the advertised frame index; checkpoints never split frames. If
`finished` is true, send no more audio/finish and wait for the cached final.
After restart/expiry/eviction an absent ID starts with `accepted_frames:0`;
replay the exact original encoded frames from sequence 0, including pre-skip.
Keep the same ID for transport errors/busy; use a new ID after session/inference
errors or pairing changes. Never create a fresh decoder for a retained suffix.

Unfinished disconnected sessions wait 30 seconds (2 seconds with no frames).
Accepted finish permits offline inference within a 480 second overall deadline.
Terminal results expire after 90 seconds; acknowledgment erases them early,
and the bounded 8 entry cache may evict terminal results under pressure.
Explicit cancel or a new ID abandoning a disconnected unfinished predecessor
closes input, but daemon model admission remains held until in-flight inference
joins. Network loss may delay cancellation until expiry. No audio or transcript
is written to disk. Android process death loses its in-memory recording/result.
The actor command queue has 8 slots plus one pending socket frame; the duration
bounded float spool uses 100 ms chunks (at most 19.2 MB) and bounded worker queues.
Backpressure keeps consuming result/cancel events so it cannot deadlock inference.

## Recognition and ownership

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

Streaming backends other than Parakeet Unified, Whisper CLI mode, on-demand
loading, GPU isolation, eager processing, meeting mode and secondary models are
rejected. No fallback model loads. Desktop Parakeet sessions also commit only
the completed transcript, so cumulative previews never cause duplicate typing
and hold-to-talk remains usable.
Desktop Unified stop/release retains the final destination and joins its worker
before passing the complete transcript to the normal desktop output pipeline.
This preserves CLI overrides, modifier-release handling, profiles and file
completion results. Its public state is `recording` while capturing and
`transcribing` while draining, for existing status-bar consumers. Explicit cancel
still discards the pending result. Phone sessions never use this desktop pipeline.

## Timing diagnostics

Completion logs contain only counters and durations, never transcript text or
authentication data. `Mobile resumable session completed` reports received audio seconds,
wall time and time from receiving `finish` to the terminal event. `Mobile
streaming worker completed` reports its corresponding timings and accumulated
wait for inference queue capacity. A large transport/worker timing gap suggests
queue drain or daemon post-processing; low daemon times with high phone delay
suggest transport or client/editor work. Cancellation can omit a terminal
transport record while still producing the worker completion record.

## Capability discovery

Authenticated `ws://127.0.0.1:8765/v1/capabilities` sends one event and closes,
without requesting admission or constructing a model:

```json
{"type":"capabilities","protocol":2,"dictation":true,"sample_rate":16000,"channels":1,"format":"opus","results":["partial","final"],"max_seconds":300,"resume":true,"audio_encodings":["opus_v1"],"framing":"sequence_opus_v1","max_encoded_bytes":2097152}
```

Other backend integrations must upgrade to protocol 2, encode Opus, and retain
complete frames for recovery. Existing protocol 1 integrations fail clearly;
there is no maintained PCM client path. Credentials remain on the trusted backend.
