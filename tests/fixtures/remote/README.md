# Opus protocol 2 golden fixture

`opus-v1.bin` contains two wire frames, each prefixed by a u32 big-endian length.
It encodes 1601 samples of a 440 Hz sine at 16 kHz, amplitude 12000 signed 16-bit.
Source sample i is `int(12000 * sin(2*pi*440*i/16000))`. The host libopus 1.6.1
encoder used VOIP, 24 kbit/s unconstrained VBR, complexity 10, DTX/FEC disabled,
20 ms packets and queried lookahead 104 (312 in 48 kHz pre-skip units). Zero padding
to 1920 input samples flushes the lookahead; the final original count is 1601.
This is a synthetic codec-only fixture, with no speech or model inference.

Rust validates native decoding, first/tail energy, sample alignment and exact
trimming; the session actor compares uninterrupted output with reconnect,
duplicate and stale-socket output. Android rebuilds the identical production
frames from these packets. Its Gradle unit-test property points to this fixture.
These checks do not validate Android MediaCodec behavior or phone recognition.
