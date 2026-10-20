//! Zero-allocation gzip decompression for Huobi WebSocket binary frames.

use flate2::{Decompress, FlushDecompress, Status};
use std::fmt;

/// Errors encountered during gzip decompression of WebSocket binary frames.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GzipError {
    /// Input frame is shorter than the minimum gzip header and trailer (18 bytes).
    #[error("input data too short for gzip frame: len={0}")]
    TooShort(usize),

    /// Missing or invalid gzip magic bytes (0x1f, 0x8b) or compression method (0x08).
    #[error("invalid gzip header: expected magic 0x1f 0x8b and CM 0x08")]
    InvalidHeader,

    /// Extra headers (FEXTRA, FNAME, FCOMMENT) are corrupted or truncated.
    #[error("truncated gzip header extension")]
    TruncatedHeader,

    /// Decompressed size exceeded configured frame size limit.
    #[error("decompressed frame size exceeded limit of {limit} bytes")]
    FrameTooLarge {
        /// Configured limit in bytes.
        limit: usize,
    },

    /// Decompression failed in flate2 decompressor.
    #[error("decompression failed: {0}")]
    DecompressFailed(String),

    /// Decompressed output size did not match the gzip ISIZE trailer.
    #[error("gzip isize mismatch: expected {expected}, got {actual}")]
    SizeMismatch {
        /// Expected uncompressed size from trailer.
        expected: usize,
        /// Actual uncompressed bytes produced.
        actual: usize,
    },
}

/// Default maximum decompressed frame size allowed (1 MiB).
pub const DEFAULT_MAX_DECOMPRESSED_FRAME_SIZE: usize = 1024 * 1024;

/// Reusable gzip frame decoder.
///
/// Keeps an internal decompression scratch buffer and reuses a [`flate2::Decompress`]
/// state machine to guarantee zero heap allocations after warm-up.
pub struct GzipDecoder {
    decompressor: Decompress,
    buffer: Vec<u8>,
    max_frame_size: usize,
}

impl fmt::Debug for GzipDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GzipDecoder")
            .field("buffer_capacity", &self.buffer.capacity())
            .field("buffer_len", &self.buffer.len())
            .field("max_frame_size", &self.max_frame_size)
            .finish()
    }
}

impl Default for GzipDecoder {
    fn default() -> Self {
        Self::with_capacity_and_limit(64 * 1024, DEFAULT_MAX_DECOMPRESSED_FRAME_SIZE)
    }
}

impl GzipDecoder {
    /// Creates a new `GzipDecoder` with default 64 KiB scratch capacity and 1 MiB limit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `GzipDecoder` pre-allocating `capacity` bytes for uncompressed output.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_limit(capacity, DEFAULT_MAX_DECOMPRESSED_FRAME_SIZE)
    }

    /// Creates a new `GzipDecoder` with pre-allocated `capacity` and custom `max_frame_size`.
    pub fn with_capacity_and_limit(capacity: usize, max_frame_size: usize) -> Self {
        Self {
            decompressor: Decompress::new(false),
            buffer: Vec::with_capacity(capacity.min(max_frame_size)),
            max_frame_size,
        }
    }

    /// Returns the configured maximum decompressed frame size.
    pub fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    /// Sets the maximum decompressed frame size.
    pub fn set_max_frame_size(&mut self, max_frame_size: usize) {
        self.max_frame_size = max_frame_size;
        if self.buffer.capacity() > self.max_frame_size {
            self.buffer.shrink_to(self.max_frame_size);
        }
    }

    /// Returns the current scratch buffer capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Decompresses a gzip-compressed binary WebSocket frame into the reused buffer.
    ///
    /// Returns a borrowed slice of the uncompressed bytes valid until the next call.
    pub fn decompress<'a>(&'a mut self, input: &[u8]) -> Result<&'a [u8], GzipError> {
        if input.len() < 18 {
            return Err(GzipError::TooShort(input.len()));
        }

        // Validate gzip magic bytes: ID1 = 0x1f, ID2 = 0x8b, CM = 0x08 (deflate)
        if input[0] != 0x1f || input[1] != 0x8b || input[2] != 0x08 {
            return Err(GzipError::InvalidHeader);
        }

        let flags = input[3];
        let mut pos = 10; // fixed header size

        // FEXTRA
        if flags & 0x04 != 0 {
            if input.len() < pos + 2 {
                return Err(GzipError::TruncatedHeader);
            }
            let xlen = u16::from_le_bytes([input[pos], input[pos + 1]]) as usize;
            pos += 2 + xlen;
            if input.len() < pos {
                return Err(GzipError::TruncatedHeader);
            }
        }

        // FNAME (null-terminated)
        if flags & 0x08 != 0 {
            loop {
                if pos >= input.len() {
                    return Err(GzipError::TruncatedHeader);
                }
                let b = input[pos];
                pos += 1;
                if b == 0 {
                    break;
                }
            }
        }

        // FCOMMENT (null-terminated)
        if flags & 0x10 != 0 {
            loop {
                if pos >= input.len() {
                    return Err(GzipError::TruncatedHeader);
                }
                let b = input[pos];
                pos += 1;
                if b == 0 {
                    break;
                }
            }
        }

        // FHCRC (2 bytes)
        if flags & 0x02 != 0 {
            pos += 2;
            if input.len() < pos {
                return Err(GzipError::TruncatedHeader);
            }
        }

        // Read uncompressed ISIZE from the last 4 bytes of trailer
        let trailer_start = input.len() - 8;
        if pos > trailer_start {
            return Err(GzipError::TruncatedHeader);
        }

        let isize_bytes = [
            input[input.len() - 4],
            input[input.len() - 3],
            input[input.len() - 2],
            input[input.len() - 1],
        ];
        let expected_size = u32::from_le_bytes(isize_bytes) as usize;

        // Cap enforced before reserve/inflation, completely ignoring trailer ISIZE as an allocation hint
        if expected_size > self.max_frame_size {
            return Err(GzipError::FrameTooLarge {
                limit: self.max_frame_size,
            });
        }

        let deflate_data = &input[pos..trailer_start];

        self.buffer.clear();
        if self.buffer.capacity() > self.max_frame_size {
            self.buffer.shrink_to(self.max_frame_size);
        }

        self.decompressor.reset(false);

        let mut chunk = [0u8; 16384];
        loop {
            let before_in = self.decompressor.total_in() as usize;
            let before_out = self.decompressor.total_out() as usize;

            if before_in > deflate_data.len() {
                return Err(GzipError::DecompressFailed(
                    "decompressor read past deflate data boundary".to_string(),
                ));
            }

            let in_slice = &deflate_data[before_in..];
            let status = self
                .decompressor
                .decompress(in_slice, &mut chunk, FlushDecompress::None)
                .map_err(|e| GzipError::DecompressFailed(e.to_string()))?;

            let after_out = self.decompressor.total_out() as usize;
            let written = after_out.saturating_sub(before_out);

            if self.buffer.len().saturating_add(written) > self.max_frame_size {
                return Err(GzipError::FrameTooLarge {
                    limit: self.max_frame_size,
                });
            }

            self.buffer.extend_from_slice(&chunk[..written]);

            match status {
                Status::StreamEnd => break,
                // `BufError` only means no progress was possible on this call; a full
                // output chunk is reported as `Ok`, so both are judged by progress.
                Status::Ok | Status::BufError => {
                    let after_in = self.decompressor.total_in() as usize;
                    if after_in == before_in && written == 0 {
                        return Err(GzipError::DecompressFailed(
                            "decompressor made no forward progress".to_string(),
                        ));
                    }
                }
            }
        }

        if self.buffer.capacity() > self.max_frame_size {
            self.buffer.shrink_to(self.max_frame_size);
        }

        if self.buffer.len() != expected_size {
            return Err(GzipError::SizeMismatch {
                expected: expected_size,
                actual: self.buffer.len(),
            });
        }

        Ok(&self.buffer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_decompress_depth_update_gz() {
        let gz = include_bytes!("../../tests/fixtures/huobi/depth_update.json.gz");
        let expected = include_bytes!("../../tests/fixtures/huobi/depth_update.json");

        let mut decoder = GzipDecoder::new();
        let uncompressed = decoder.decompress(gz).unwrap();

        assert_eq!(uncompressed, expected);
    }

    #[test]
    fn test_decompress_reused_buffer_no_growth() {
        let gz = include_bytes!("../../tests/fixtures/huobi/depth_update.json.gz");
        let mut decoder = GzipDecoder::with_capacity(4096);
        let cap_before = decoder.capacity();

        let res1 = decoder.decompress(gz).unwrap();
        assert!(!res1.is_empty());
        assert_eq!(decoder.capacity(), cap_before);

        let res2 = decoder.decompress(gz).unwrap();
        assert!(!res2.is_empty());
        assert_eq!(decoder.capacity(), cap_before);
    }

    #[test]
    fn test_decompress_all_fixtures() {
        let fixtures = [
            (
                &include_bytes!("../../tests/fixtures/huobi/depth_update.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/depth_update.json")[..],
            ),
            (
                &include_bytes!("../../tests/fixtures/huobi/order_book_snapshot.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/order_book_snapshot.json")[..],
            ),
            (
                &include_bytes!("../../tests/fixtures/huobi/heartbeat_or_ping.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/heartbeat_or_ping.json")[..],
            ),
            (
                &include_bytes!("../../tests/fixtures/huobi/trade.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/trade.json")[..],
            ),
            (
                &include_bytes!("../../tests/fixtures/huobi/sub_ack.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/sub_ack.json")[..],
            ),
            (
                &include_bytes!("../../tests/fixtures/huobi/depth_update_one_sided.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/depth_update_one_sided.json")[..],
            ),
            (
                &include_bytes!("../../tests/fixtures/huobi/error.json.gz")[..],
                &include_bytes!("../../tests/fixtures/huobi/error.json")[..],
            ),
        ];

        let mut decoder = GzipDecoder::new();
        for (gz, raw) in fixtures {
            let decompressed = decoder.decompress(gz).unwrap();
            assert_eq!(decompressed, raw);
        }
    }

    #[test]
    fn test_invalid_gzip_header() {
        let mut decoder = GzipDecoder::new();
        let invalid = [0u8; 20];
        assert_eq!(decoder.decompress(&invalid), Err(GzipError::InvalidHeader));
    }

    #[test]
    fn test_too_short_data() {
        let mut decoder = GzipDecoder::new();
        assert_eq!(
            decoder.decompress(&[0x1f, 0x8b]),
            Err(GzipError::TooShort(2))
        );
    }

    #[test]
    fn test_forged_trailer_huge_isize_does_not_grow_capacity() {
        let gz = include_bytes!("../../tests/fixtures/huobi/depth_update.json.gz");
        let mut forged = gz.to_vec();
        let len = forged.len();
        // Overwrite ISIZE with ~2 GiB
        forged[len - 4] = 0xff;
        forged[len - 3] = 0xff;
        forged[len - 2] = 0xff;
        forged[len - 1] = 0x7f;

        let mut decoder = GzipDecoder::with_capacity(4096);
        let cap_before = decoder.capacity();

        let res = decoder.decompress(&forged);
        assert_eq!(
            res,
            Err(GzipError::FrameTooLarge {
                limit: DEFAULT_MAX_DECOMPRESSED_FRAME_SIZE,
            })
        );
        // Ensure capacity never expanded to 2 GiB
        assert_eq!(decoder.capacity(), cap_before);
    }

    #[test]
    fn test_frame_exceeds_custom_cap() {
        let gz = include_bytes!("../../tests/fixtures/huobi/depth_update.json.gz");
        let mut decoder = GzipDecoder::with_capacity_and_limit(1024, 64);

        let res = decoder.decompress(gz);
        assert_eq!(res, Err(GzipError::FrameTooLarge { limit: 64 }));
        assert!(decoder.capacity() <= 64);
    }

    /// Deterministic printable payload that deflate cannot shrink to a few bytes.
    fn payload(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                b'!' + ((state >> 33) % 90) as u8
            })
            .collect()
    }

    fn gzip(data: &[u8]) -> Vec<u8> {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn test_decompress_output_spanning_multiple_16k_chunks() {
        // Around and across the 16 KiB scratch chunk boundary, including exact multiples.
        for len in [16 * 1024 - 1, 16 * 1024, 16 * 1024 + 1, 32 * 1024, 70_000] {
            let raw = payload(len);
            let gz = gzip(&raw);
            let mut decoder = GzipDecoder::new();
            let out = decoder.decompress(&gz).unwrap();
            assert_eq!(out.len(), raw.len(), "len {len}");
            assert_eq!(out, &raw[..], "len {len}");
        }
    }

    #[test]
    fn test_decompress_highly_compressible_multi_chunk_frame() {
        // Tiny input inflating to ~200 KiB exercises many chunk iterations on little input.
        let raw = vec![b'a'; 200 * 1024];
        let gz = gzip(&raw);
        assert!(gz.len() < 1024);
        let mut decoder = GzipDecoder::with_capacity(4096);
        assert_eq!(decoder.decompress(&gz).unwrap(), &raw[..]);
    }

    #[test]
    fn test_multi_chunk_frame_over_custom_cap_is_rejected() {
        let raw = payload(40_000);
        let gz = gzip(&raw);
        let mut decoder = GzipDecoder::with_capacity_and_limit(4096, 20_000);
        assert_eq!(
            decoder.decompress(&gz),
            Err(GzipError::FrameTooLarge { limit: 20_000 })
        );
    }

    #[test]
    fn test_multi_chunk_decoder_is_reusable() {
        let big = payload(50_000);
        let small = payload(100);
        let mut decoder = GzipDecoder::new();
        assert_eq!(decoder.decompress(&gzip(&big)).unwrap(), &big[..]);
        assert_eq!(decoder.decompress(&gzip(&small)).unwrap(), &small[..]);
        assert_eq!(decoder.decompress(&gzip(&big)).unwrap(), &big[..]);
    }

    #[test]
    fn test_truncated_multi_chunk_stream_fails_instead_of_hanging() {
        let gz = gzip(&payload(50_000));
        let mut truncated = gz[..gz.len() - 2000].to_vec();
        truncated.extend_from_slice(&gz[gz.len() - 8..]);
        let mut decoder = GzipDecoder::new();
        assert!(matches!(
            decoder.decompress(&truncated),
            Err(GzipError::DecompressFailed(_))
        ));
    }
}
