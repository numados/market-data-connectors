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

/// Reusable gzip frame decoder.
///
/// Keeps an internal decompression scratch buffer and reuses a [`flate2::Decompress`]
/// state machine to guarantee zero heap allocations after warm-up.
pub struct GzipDecoder {
    decompressor: Decompress,
    buffer: Vec<u8>,
}

impl fmt::Debug for GzipDecoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GzipDecoder")
            .field("buffer_capacity", &self.buffer.capacity())
            .field("buffer_len", &self.buffer.len())
            .finish()
    }
}

impl Default for GzipDecoder {
    fn default() -> Self {
        Self::with_capacity(64 * 1024)
    }
}

impl GzipDecoder {
    /// Creates a new `GzipDecoder` with default 64 KiB scratch capacity.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `GzipDecoder` pre-allocating `capacity` bytes for uncompressed output.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            decompressor: Decompress::new(false),
            buffer: Vec::with_capacity(capacity),
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

        let deflate_data = &input[pos..trailer_start];

        self.buffer.clear();
        if self.buffer.capacity() < expected_size {
            let add = expected_size.saturating_sub(self.buffer.capacity());
            self.buffer.reserve(add);
        }

        self.decompressor.reset(false);

        let status = self
            .decompressor
            .decompress_vec(deflate_data, &mut self.buffer, FlushDecompress::Finish)
            .map_err(|e| GzipError::DecompressFailed(e.to_string()))?;

        if status != Status::StreamEnd && status != Status::Ok {
            return Err(GzipError::DecompressFailed(format!(
                "unexpected decompress status: {status:?}"
            )));
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
}
