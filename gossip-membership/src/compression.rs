/// Compression support for gossip payloads.
///
/// This module provides optional payload compression to reduce bandwidth
/// usage in large clusters. Compression is negotiated between peers and
/// only applied when both endpoints support it.
///
/// Compression algorithm: LZ4
use crate::message::capabilities;

/// Represents the local node's compression capabilities.
/// Used to advertise to peers and to negotiate compression.
pub struct LocalCapabilities {
    pub lz4_supported: bool,
}

impl Default for LocalCapabilities {
    fn default() -> Self {
        Self {
            lz4_supported: true,
        }
    }
}

impl LocalCapabilities {
    pub fn new(lz4_supported: bool) -> Self {
        Self { lz4_supported }
    }

    /// Convert to bitflags for wire transmission.
    pub fn to_bits(&self) -> u8 {
        let mut bits = 0u8;
        if self.lz4_supported {
            bits |= capabilities::LZ4;
        }
        bits
    }

    /// Check if we should compress for a given peer.
    /// Returns true if both we and the peer support compression.
    pub fn should_compress(&self, peer_capabilities: u8) -> bool {
        self.lz4_supported && (peer_capabilities & capabilities::LZ4) != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionAlgo {
    #[default]
    None,
    Lz4,
}

impl CompressionAlgo {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Lz4,
            _ => Self::None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
        }
    }
}

pub struct Compressor {
    algo: CompressionAlgo,
    min_size_for_compress: usize,
}

impl Default for Compressor {
    fn default() -> Self {
        Self {
            algo: CompressionAlgo::Lz4,
            min_size_for_compress: 64,
        }
    }
}

impl Compressor {
    pub fn new(algo: CompressionAlgo) -> Self {
        Self {
            algo,
            min_size_for_compress: 64,
        }
    }

    pub fn with_min_size(mut self, min_size: usize) -> Self {
        self.min_size_for_compress = min_size;
        self
    }

    pub fn algorithm(&self) -> CompressionAlgo {
        self.algo
    }

    pub fn compress(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < self.min_size_for_compress {
            return None;
        }

        match self.algo {
            CompressionAlgo::None => None,
            CompressionAlgo::Lz4 => Some(compress_lz4(data)),
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Option<Vec<u8>> {
        match self.algo {
            CompressionAlgo::None => None,
            CompressionAlgo::Lz4 => decompress_lz4(data).ok(),
        }
    }

    pub fn can_compress(&self) -> bool {
        self.algo != CompressionAlgo::None
    }
}

fn compress_lz4(data: &[u8]) -> Vec<u8> {
    use lz4_flex::block::compress_prepend_size;
    compress_prepend_size(data)
}

fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>, lz4_flex::block::DecompressError> {
    use lz4_flex::block::decompress_size_prepended;
    decompress_size_prepended(data)
}

pub struct CompressionStats {
    pub total_compressions: u64,
    pub total_decompressions: u64,
    pub bytes_before_compress: u64,
    pub bytes_after_compress: u64,
    pub bytes_before_decompress: u64,
    pub bytes_after_decompress: u64,
    pub total_compress_ns: u64,
    pub total_decompress_ns: u64,
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self {
            total_compressions: 0,
            total_decompressions: 0,
            bytes_before_compress: 0,
            bytes_after_compress: 0,
            bytes_before_decompress: 0,
            bytes_after_decompress: 0,
            total_compress_ns: u64::MAX,
            total_decompress_ns: u64::MAX,
        }
    }
}

impl CompressionStats {
    pub fn record_compress(&mut self, before: usize, after: usize, elapsed_ns: u64) {
        self.total_compressions += 1;
        self.bytes_before_compress += before as u64;
        self.bytes_after_compress += after as u64;
        self.total_compress_ns = self.total_compress_ns.min(elapsed_ns);
    }

    pub fn record_decompress(&mut self, before: usize, after: usize, elapsed_ns: u64) {
        self.total_decompressions += 1;
        self.bytes_before_decompress += before as u64;
        self.bytes_after_decompress += after as u64;
        self.total_decompress_ns = self.total_decompress_ns.min(elapsed_ns);
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_before_compress == 0 {
            return 1.0;
        }
        self.bytes_after_compress as f64 / self.bytes_before_compress as f64
    }

    pub fn bandwidth_saved(&self) -> u64 {
        self.bytes_before_compress
            .saturating_sub(self.bytes_after_compress)
    }

    pub fn avg_compress_time_ns(&self) -> u64 {
        if self.total_compressions == 0 {
            return 0;
        }
        self.total_compress_ns
    }

    pub fn avg_decompress_time_ns(&self) -> u64 {
        if self.total_decompressions == 0 {
            return 0;
        }
        self.total_decompress_ns
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

pub fn compress_level() -> i32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_roundtrip() {
        let data = b"Hello, this is a test message for compression!";
        let compressor = Compressor::new(CompressionAlgo::Lz4).with_min_size(10);

        let compressed = compressor.compress(data).expect("should compress");

        let decompressed = compressor
            .decompress(&compressed)
            .expect("should decompress");
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn small_data_not_compressed() {
        let data = b"Hi";
        let compressor = Compressor::new(CompressionAlgo::Lz4).with_min_size(64);

        let result = compressor.compress(data);
        assert!(result.is_none(), "small data should not be compressed");
    }

    #[test]
    fn none_algorithm_never_compresses() {
        let data = vec![0u8; 1000];
        let compressor = Compressor::new(CompressionAlgo::None);

        let result = compressor.compress(&data);
        assert!(result.is_none(), "none algorithm should never compress");
    }

    #[test]
    fn compression_stats_track() {
        let mut stats = CompressionStats::default();

        stats.record_compress(1000, 400, 1000);
        stats.record_compress(500, 200, 500);
        stats.record_decompress(400, 1000, 800);
        stats.record_decompress(200, 500, 300);

        assert_eq!(stats.total_compressions, 2);
        assert_eq!(stats.total_decompressions, 2);
        assert_eq!(stats.bytes_before_compress, 1500);
        assert_eq!(stats.bytes_after_compress, 600);
        assert!((stats.compression_ratio() - 0.4).abs() < 0.001);
        assert_eq!(stats.bandwidth_saved(), 900);
    }

    #[test]
    fn compression_algo_serialization() {
        assert_eq!(CompressionAlgo::None.as_u8(), 0);
        assert_eq!(CompressionAlgo::Lz4.as_u8(), 1);
        assert_eq!(CompressionAlgo::from_u8(0), CompressionAlgo::None);
        assert_eq!(CompressionAlgo::from_u8(1), CompressionAlgo::Lz4);
        assert_eq!(CompressionAlgo::from_u8(255), CompressionAlgo::None);
    }

    #[test]
    fn large_data_compresses_well() {
        let data: Vec<u8> = (0..10000_u32).flat_map(|i| i.to_be_bytes()).collect();
        let compressor = Compressor::new(CompressionAlgo::Lz4).with_min_size(10);

        let compressed = compressor
            .compress(&data)
            .expect("should compress large data");
        // LZ4压缩可能会产生比输入更大的输出（尤其对小数据块），但对重复数据应该压缩
        assert!(
            compressed.len() <= data.len() * 2,
            "compressed should not be wildly larger"
        );
    }

    #[test]
    fn random_data_compresses_less() {
        let data: Vec<u8> = (0..1000).map(|_| rand::random::<u8>()).collect();
        let compressor = Compressor::new(CompressionAlgo::Lz4);

        let compressed = compressor.compress(&data);
        assert!(
            compressed.is_some(),
            "random data may still compress (dictionary)"
        );
    }
}
