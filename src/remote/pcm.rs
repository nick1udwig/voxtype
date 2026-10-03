//! Negotiated lossless framing changes only transport bytes, never PCM or offsets.
use flate2::{Decompress, FlushDecompress, Status};
use serde_json::{json, Value};
use std::borrow::Cow;

pub(super) const ENCODING: &str = "pcm_s16le_zlib_v1";
// Leaves room for the 8-byte resume offset and 5-byte envelope in a 64-KiB message.
pub(super) const MAX_DECODED_BYTES: usize = 65522;

pub(super) fn ready(event: &mut Value, compressed: bool) {
    if compressed && event["type"] == "ready" {
        event["audio_encoding"] = json!(ENCODING);
    }
}

pub(super) fn decode(bytes: &[u8], compressed: bool) -> Result<Cow<'_, [u8]>, ()> {
    if !compressed {
        return Ok(Cow::Borrowed(bytes));
    }
    if bytes.len() < 5 || bytes.len() > 65536 {
        return Err(());
    }
    let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    if length == 0 || length % 2 != 0 || length > MAX_DECODED_BYTES {
        return Err(());
    }
    let payload = &bytes[5..];
    match bytes[0] {
        0 if payload.len() == length => Ok(Cow::Borrowed(payload)),
        1 => {
            // One extra byte detects a dishonest decoded-length claim. Neither
            // an oversized claim nor a compressed bomb can allocate beyond this cap.
            let mut pcm = vec![0; length + 1];
            let mut decoder = Decompress::new(true);
            let status = decoder
                .decompress(payload, &mut pcm, FlushDecompress::Finish)
                .map_err(|_| ())?;
            if status != Status::StreamEnd
                || decoder.total_in() != payload.len() as u64
                || decoder.total_out() != length as u64
            {
                return Err(());
            }
            pcm.truncate(length);
            Ok(Cow::Owned(pcm))
        }
        _ => Err(()),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use flate2::{write::ZlibEncoder, Compression};
    use std::io::Write;

    pub(crate) fn frame(pcm: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(pcm).unwrap();
        let mut bytes = vec![1];
        bytes.extend((pcm.len() as u32).to_be_bytes());
        bytes.extend(encoder.finish().unwrap());
        bytes
    }

    #[test]
    fn independent_frames_and_raw_fallback_are_exact() {
        let pcm = vec![0; 3200];
        let compressed = frame(&pcm);
        assert_eq!(decode(&compressed, true).unwrap().as_ref(), pcm);
        let mut raw = vec![0];
        raw.extend((pcm.len() as u32).to_be_bytes());
        raw.extend(&pcm);
        assert_eq!(decode(&raw, true).unwrap().as_ref(), pcm);
        assert_eq!(decode(&pcm, false).unwrap().as_ref(), pcm);
        assert_eq!(
            decode(&frame(&[0, 128, 255, 127]), true).unwrap().as_ref(),
            [0, 128, 255, 127]
        );
    }

    #[test]
    fn java_and_python_zlib_golden_vector_decodes_exactly() {
        // Fixed vector shared with Android LosslessPcmTest (zlib level 1).
        let hex = "010000004078016368f85fcfc0c0c8c040260d001f360ff9";
        let frame: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let pcm = [0, 128, 255, 127, 0, 0, 1, 0].repeat(8);
        assert_eq!(decode(&frame, true).unwrap().as_ref(), pcm);
    }

    #[test]
    fn invalid_streams_lengths_and_bombs_are_rejected() {
        let valid = frame(&vec![0; 3200]);
        for cut in 0..valid.len() {
            assert!(decode(&valid[..cut], true).is_err());
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode(&trailing, true).is_err());
        let mut concatenated = valid.clone();
        concatenated.extend(&valid[5..]);
        assert!(decode(&concatenated, true).is_err());
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode(&corrupt, true).is_err());
        for length in [0u32, 1, 2, 3198, 3202, 65524, u32::MAX] {
            let mut false_length = valid.clone();
            false_length[1..5].copy_from_slice(&length.to_be_bytes());
            assert!(decode(&false_length, true).is_err());
        }
        let mut unknown = valid.clone();
        unknown[0] = 2;
        assert!(decode(&unknown, true).is_err());
        let mut bomb = frame(&vec![0; MAX_DECODED_BYTES * 16]);
        bomb[1..5].copy_from_slice(&(MAX_DECODED_BYTES as u32).to_be_bytes());
        assert!(decode(&bomb, true).is_err());
        let mut dictionary = valid;
        dictionary[5..7].copy_from_slice(&[0x78, 0x20]);
        assert!(decode(&dictionary, true).is_err());
        assert!(decode(&[0, 0, 0, 0, 2, 0], true).is_err());
        assert!(decode(&[0, 0, 0, 0, 1, 0], true).is_err());
        assert_eq!(
            decode(&frame(&vec![0; MAX_DECODED_BYTES]), true)
                .unwrap()
                .len(),
            MAX_DECODED_BYTES
        );
    }
}
