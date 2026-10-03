//! Mandatory Opus wire transport. Native state belongs to one session actor.
use libloading::Library;
use std::ffi::c_void;
use std::sync::{Arc, OnceLock};

pub(super) const ENCODING: &str = "opus_v1";
pub(super) const MAX_PACKET: usize = 1275;
pub(super) const MAX_PACKETS: usize = 5;
pub(super) const MAX_FRAME: usize = 17 + MAX_PACKETS * (2 + MAX_PACKET);
pub(super) const MAX_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_FRAMES: usize = 15002;
const PACKET_SAMPLES: usize = 320;
const MAX_SKIP_48K: usize = 3840;

type Create = unsafe extern "C" fn(i32, i32, *mut i32) -> *mut c_void;
type Destroy = unsafe extern "C" fn(*mut c_void);
type Decode = unsafe extern "C" fn(*mut c_void, *const u8, i32, *mut f32, i32, i32) -> i32;
type Samples = unsafe extern "C" fn(*const u8, i32, i32) -> i32;
type Channels = unsafe extern "C" fn(*const u8) -> i32;
struct Api {
    create: Create,
    destroy: Destroy,
    decode: Decode,
    samples: Samples,
    channels: Channels,
    _library: Library,
}
impl Api {
    fn load() -> Result<Arc<Self>, &'static str> {
        static API: OnceLock<Result<Arc<Api>, &'static str>> = OnceLock::new();
        API.get_or_init(|| {
            // Only known system sonames, never a user-controlled path. Symbols stay
            // alive through the Arc held by each decoder, including during Drop.
            #[cfg(target_os = "linux")]
            let names = ["libopus.so.0"];
            #[cfg(target_os = "macos")]
            let names = ["libopus.0.dylib", "libopus.dylib"];
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let names = ["opus.dll"];
            for name in names {
                unsafe {
                    let Ok(library) = Library::new(name) else {
                        continue;
                    };
                    let api = Self {
                        create: *library
                            .get(b"opus_decoder_create\0")
                            .map_err(|_| "codec_unavailable")?,
                        destroy: *library
                            .get(b"opus_decoder_destroy\0")
                            .map_err(|_| "codec_unavailable")?,
                        decode: *library
                            .get(b"opus_decode_float\0")
                            .map_err(|_| "codec_unavailable")?,
                        samples: *library
                            .get(b"opus_packet_get_nb_samples\0")
                            .map_err(|_| "codec_unavailable")?,
                        channels: *library
                            .get(b"opus_packet_get_nb_channels\0")
                            .map_err(|_| "codec_unavailable")?,
                        _library: library,
                    };
                    return Ok(Arc::new(api));
                }
            }
            Err("codec_unavailable")
        })
        .clone()
    }
}

pub(super) struct Frame<'a> {
    pub sequence: usize,
    first: bool,
    final_frame: bool,
    skip: usize,
    total: usize,
    packets: Vec<&'a [u8]>,
}
impl<'a> Frame<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, ()> {
        if !(17..=MAX_FRAME).contains(&bytes.len()) || bytes[8] != 1 || bytes[9] > 3 {
            return Err(());
        }
        let sequence =
            usize::try_from(u64::from_be_bytes(bytes[..8].try_into().unwrap())).map_err(|_| ())?;
        let first = bytes[9] & 1 != 0;
        let final_frame = bytes[9] & 2 != 0;
        let skip48 = u16::from_be_bytes(bytes[10..12].try_into().unwrap()) as usize;
        let total = u32::from_be_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let count = bytes[16] as usize;
        if sequence >= MAX_FRAMES
            || count > MAX_PACKETS
            || (count == 0 && !final_frame)
            || skip48 > MAX_SKIP_48K
            || skip48 % 3 != 0
            || (!first && skip48 != 0)
            || (!final_frame && total != 0)
            || total > 300 * 16000
        {
            return Err(());
        }
        let mut packets = Vec::with_capacity(count);
        let mut offset = 17;
        for _ in 0..count {
            if offset + 2 > bytes.len() {
                return Err(());
            }
            let length = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if !(1..=MAX_PACKET).contains(&length) || offset + length > bytes.len() {
                return Err(());
            }
            packets.push(&bytes[offset..offset + length]);
            offset += length;
        }
        if offset != bytes.len() {
            return Err(());
        }
        Ok(Self {
            sequence,
            first,
            final_frame,
            skip: skip48 / 3,
            total,
            packets,
        })
    }
}

pub(super) struct Decoder {
    api: Arc<Api>,
    state: *mut c_void,
    started: bool,
    pub finished: bool,
    skip: usize,
    decoded: usize,
    pub delivered: usize,
    tail: Vec<f32>,
}
// Ownership moves into a single actor. No pointer or shared access escapes it;
// libopus decoder state is never accessed concurrently. Decoder is not Sync.
unsafe impl Send for Decoder {}
impl Decoder {
    pub fn new() -> Result<Self, &'static str> {
        let api = Api::load()?;
        let mut error = 0;
        let state = unsafe { (api.create)(16000, 1, &mut error) };
        if state.is_null() || error != 0 {
            return Err("codec_unavailable");
        }
        Ok(Self {
            api,
            state,
            started: false,
            finished: false,
            skip: 0,
            decoded: 0,
            delivered: 0,
            tail: Vec::new(),
        })
    }
    pub fn decode(&mut self, frame: &Frame<'_>, max_samples: usize) -> Result<Vec<f32>, ()> {
        if self.finished
            || frame.first == self.started
            || (frame.first && frame.sequence != 0)
            || frame.total > max_samples
        {
            return Err(());
        }
        // Validate every packet and cumulative duration before mutating native state.
        for packet in &frame.packets {
            if unsafe { (self.api.channels)(packet.as_ptr()) } != 1
                || unsafe { (self.api.samples)(packet.as_ptr(), packet.len() as i32, 16000) }
                    != PACKET_SAMPLES as i32
            {
                return Err(());
            }
        }
        let skip = if frame.first { frame.skip } else { self.skip };
        let new_samples = frame.packets.len() * PACKET_SAMPLES;
        if self.decoded + new_samples > max_samples + MAX_SKIP_48K / 3 + 2 * PACKET_SAMPLES
            || (frame.final_frame
                && (frame.total < self.delivered
                    || frame.total
                        > self.delivered + self.tail.len() + new_samples.saturating_sub(skip)))
        {
            return Err(());
        }
        if frame.first {
            self.skip = frame.skip;
            self.started = true;
        }
        let mut output = std::mem::take(&mut self.tail);
        for packet in &frame.packets {
            let mut pcm = [0f32; PACKET_SAMPLES];
            let count = unsafe {
                (self.api.decode)(
                    self.state,
                    packet.as_ptr(),
                    packet.len() as i32,
                    pcm.as_mut_ptr(),
                    PACKET_SAMPLES as i32,
                    0,
                )
            };
            if count != PACKET_SAMPLES as i32 {
                return Err(());
            }
            self.decoded += PACKET_SAMPLES;
            let trim = self.skip.min(PACKET_SAMPLES);
            self.skip -= trim;
            output.extend_from_slice(&pcm[trim..]);
        }
        if frame.final_frame {
            let remaining = frame.total.checked_sub(self.delivered).ok_or(())?;
            if remaining > output.len() || self.skip != 0 {
                return Err(());
            }
            output.truncate(remaining);
            self.finished = true;
        } else {
            // Keep one packet until EOS, including when MediaCodec emits empty EOS.
            // At most 20ms padding can then be trimmed without retracting model input.
            let keep = output.len().min(PACKET_SAMPLES);
            self.tail = output.split_off(output.len() - keep);
        }
        self.delivered += output.len();
        if self.delivered > max_samples {
            return Err(());
        }
        Ok(output)
    }
}
impl Drop for Decoder {
    fn drop(&mut self) {
        unsafe { (self.api.destroy)(self.state) };
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    pub fn frame(
        sequence: u64,
        count: usize,
        first: bool,
        final_frame: bool,
        total: usize,
    ) -> Vec<u8> {
        let mut bytes = sequence.to_be_bytes().to_vec();
        bytes.extend([1, first as u8 | ((final_frame as u8) << 1)]);
        bytes.extend(0u16.to_be_bytes());
        bytes.extend((total as u32).to_be_bytes());
        bytes.push(count as u8);
        for _ in 0..count {
            bytes.extend([0, 3, 0xf8, 0xff, 0xfe]);
        }
        bytes
    }
    #[test]
    fn strict_bounds_and_empty_eos() {
        let mut decoder = Decoder::new().unwrap();
        assert_eq!(
            decoder
                .decode(&Frame::parse(&frame(0, 0, true, true, 0)).unwrap(), 16000)
                .unwrap()
                .len(),
            0
        );
        for bytes in [
            frame(0, 0, true, false, 0),
            frame(0, 6, true, false, 0),
            vec![0; 16],
            vec![0; MAX_FRAME + 1],
        ] {
            assert!(Frame::parse(&bytes).is_err());
        }
        let mut decoder = Decoder::new().unwrap();
        assert!(decoder
            .decode(&Frame::parse(&frame(0, 0, true, true, 1)).unwrap(), 16000)
            .is_err());
        let mut bad = frame(0, 1, true, true, 320);
        bad.pop();
        assert!(Frame::parse(&bad).is_err());
        let mut bad = frame(0, 1, true, true, 320);
        bad.push(0);
        assert!(Frame::parse(&bad).is_err());
    }
    #[test]
    fn tail_trim_and_duration_limit() {
        let mut decoder = Decoder::new().unwrap();
        assert_eq!(
            decoder
                .decode(&Frame::parse(&frame(0, 5, true, false, 0)).unwrap(), 1600)
                .unwrap()
                .len(),
            1280
        );
        assert_eq!(
            decoder
                .decode(
                    &Frame::parse(&frame(1, 0, false, true, 1500)).unwrap(),
                    1600
                )
                .unwrap()
                .len(),
            220
        );
        assert_eq!(decoder.delivered, 1500);
        assert!(decoder
            .decode(
                &Frame::parse(&frame(2, 1, false, true, 1600)).unwrap(),
                1600
            )
            .is_err());
        let mut decoder = Decoder::new().unwrap();
        assert!(decoder
            .decode(&Frame::parse(&frame(0, 5, true, true, 1600)).unwrap(), 1599)
            .is_err());
    }
    #[test]
    fn malformed_packet_and_unrestorable_duration() {
        let mut bad = frame(0, 1, true, true, 320);
        bad[19] = 0xff;
        assert!(Decoder::new()
            .unwrap()
            .decode(&Frame::parse(&bad).unwrap(), 16000)
            .is_err());
        assert!(Decoder::new()
            .unwrap()
            .decode(&Frame::parse(&frame(0, 1, true, true, 321)).unwrap(), 16000)
            .is_err());
    }
    pub fn golden() -> Vec<Vec<u8>> {
        let mut bytes = include_bytes!("../../tests/fixtures/remote/opus-v1.bin").as_slice();
        let mut frames = Vec::new();
        while !bytes.is_empty() {
            let length = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
            frames.push(bytes[4..4 + length].to_vec());
            bytes = &bytes[4 + length..];
        }
        frames
    }
    #[test]
    fn real_opus_golden_preserves_first_and_tail_tone_and_exact_original_samples() {
        let mut decoder = Decoder::new().unwrap();
        let mut pcm = Vec::new();
        for bytes in golden() {
            pcm.extend(
                decoder
                    .decode(&Frame::parse(&bytes).unwrap(), 16000)
                    .unwrap(),
            );
        }
        assert_eq!(pcm.len(), 1601);
        // 440Hz source begins at sample0. Encoder startup can attenuate a few
        // samples, but both boundaries must retain tone, not silence or padding.
        for window in [&pcm[32..192], &pcm[1440..1600]] {
            let energy = window.iter().map(|s| s * s).sum::<f32>() / window.len() as f32;
            assert!(
                energy > 0.025 && energy < 0.15,
                "boundary tone energy {energy}"
            );
        }
        let correlation = pcm
            .iter()
            .enumerate()
            .map(|(i, s)| *s * (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 16000.0).sin())
            .sum::<f32>()
            / pcm.len() as f32;
        assert!(
            correlation > 0.1,
            "pre-skip alignment correlation {correlation}"
        );
    }
}
