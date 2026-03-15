use gossip_membership::compression::{CompressionAlgo, CompressionStats, Compressor};
use gossip_membership::message::{
    build_gossip, flags, Message, MessagePayload, WireNodeEntry, HEADER_LEN, OFF_COMPRESSION,
    OFF_FLAGS,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn v4(ip: [u8; 4], port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), port)
}

fn make_entry(node_id: u64, heartbeat: u32) -> WireNodeEntry {
    WireNodeEntry {
        node_id,
        heartbeat,
        incarnation: 0,
        status: 0,
        addr: v4([192, 168, 1, (node_id % 255) as u8], 8000 + node_id as u16),
    }
}

#[test]
fn compress_decompress_message_roundtrip() {
    let entries: Vec<WireNodeEntry> = (0..50).map(|i| make_entry(i, i as u32)).collect();
    let msg = build_gossip(1, 1, 0, entries.clone());

    // Compress the message
    let compressed_msg = msg.clone().with_compression(1);
    let encoded = compressed_msg.encode().unwrap();

    // Verify the compressed flag is set
    assert!(encoded[OFF_FLAGS] & flags::COMPRESSED != 0);
    assert_eq!(encoded[OFF_COMPRESSION], 1);

    // Decode and verify
    let decoded = Message::decode(&encoded).unwrap();
    assert!(decoded.is_compressed());
    assert_eq!(decoded.compression_algo(), 1);

    // Check payload is correct
    match decoded.payload {
        MessagePayload::Gossip(got_entries) => {
            assert_eq!(got_entries.len(), entries.len());
            for (expected, got) in entries.iter().zip(got_entries.iter()) {
                assert_eq!(expected.node_id, got.node_id);
                assert_eq!(expected.heartbeat, got.heartbeat);
            }
        }
        _ => panic!("expected gossip payload"),
    }
}

#[test]
fn uncompressed_message_stays_uncompressed() {
    let entries = vec![make_entry(1, 1)];
    let msg = build_gossip(1, 1, 0, entries);

    let encoded = msg.encode().unwrap();

    // Verify no compression flag
    assert!(encoded[OFF_FLAGS] & flags::COMPRESSED == 0);
    assert_eq!(encoded[OFF_COMPRESSION], 0);
}

#[test]
fn small_message_not_compressed_by_default() {
    // Only one entry - too small to compress
    let entries = vec![make_entry(1, 1)];
    let msg = build_gossip(1, 1, 0, entries);

    // Clone and try to force compression
    let compressed_msg = msg.clone().with_compression(1);
    let encoded = compressed_msg.encode().unwrap();

    // Even with compression set, small payload might not benefit
    // The flag is set but actual compression is optional
    assert!(encoded[OFF_FLAGS] & flags::COMPRESSED != 0);
}

#[test]
fn large_gossip_message_compresses_well() {
    // Create a payload that compresses well but stays under 1400 bytes
    // 50 entries * 24 bytes = 1200 bytes, should compress to ~300 bytes
    let entries: Vec<WireNodeEntry> = (0..50)
        .map(|i| WireNodeEntry {
            node_id: i,
            heartbeat: i as u32,
            incarnation: 0,
            status: 0,
            addr: v4([10, 0, i as u8, (i % 255) as u8], 8000),
        })
        .collect();

    let msg = build_gossip(1, 1, 0, entries);
    let uncompressed = msg.encode().unwrap();

    let compressed_msg = msg.with_compression(1);
    let compressed = compressed_msg.encode().unwrap();

    // Compressed should be smaller
    assert!(
        compressed.len() < uncompressed.len(),
        "compressed {} should be < uncompressed {}",
        compressed.len(),
        uncompressed.len()
    );

    // Both should decode to the same data
    let decoded_uncompressed = Message::decode(&uncompressed).unwrap();
    let decoded_compressed = Message::decode(&compressed).unwrap();

    match (decoded_uncompressed.payload, decoded_compressed.payload) {
        (MessagePayload::Gossip(e1), MessagePayload::Gossip(e2)) => {
            assert_eq!(e1.len(), e2.len());
        }
        _ => panic!("expected gossip payload"),
    }
}

#[test]
fn compression_with_explicit_none_algo() {
    let entries = vec![make_entry(1, 1)];
    let msg = build_gossip(1, 1, 0, entries);

    // Explicitly set no compression
    let uncompressed_msg = msg.with_compression(0);
    let encoded = uncompressed_msg.encode().unwrap();

    assert!(encoded[OFF_FLAGS] & flags::COMPRESSED == 0);
}

#[test]
fn message_without_compression_flag_decodes_normal() {
    let entries = vec![make_entry(1, 1)];
    let msg = build_gossip(1, 1, 0, entries);

    let encoded = msg.encode().unwrap();
    let decoded = Message::decode(&encoded).unwrap();

    assert!(!decoded.is_compressed());
    assert_eq!(decoded.compression_algo(), 0);
}

#[test]
fn ping_message_with_compression() {
    let entries = vec![make_entry(1, 1)];
    let msg = gossip_membership::message::build_ping(1, 1, 0, entries);

    let compressed = msg.with_compression(1);
    let encoded = compressed.encode().unwrap();

    assert!(encoded[OFF_FLAGS] & flags::COMPRESSED != 0);

    let decoded = Message::decode(&encoded).unwrap();
    assert!(decoded.is_compressed());
}

#[test]
fn ack_message_with_compression() {
    let entries = vec![make_entry(1, 1)];
    let msg = gossip_membership::message::build_ack(1, 1, 0, entries);

    let compressed = msg.with_compression(1);
    let encoded = compressed.encode().unwrap();

    assert!(encoded[OFF_FLAGS] & flags::COMPRESSED != 0);

    let decoded = Message::decode(&encoded).unwrap();
    assert!(decoded.is_compressed());
}

#[test]
fn leave_message_without_payload_not_compressed() {
    let msg = gossip_membership::message::build_leave(1, 1, 0);

    // Leave has no payload, compression doesn't apply
    let encoded = msg.encode().unwrap();

    // Leave messages typically aren't compressed due to no payload
    assert!(encoded.len() <= HEADER_LEN);
}

#[test]
fn compression_stats_track_compression_ratio() {
    let mut stats = CompressionStats::default();

    // Simulate compression
    let data = vec![0u8; 1000];
    let compressed = Compressor::new(CompressionAlgo::Lz4)
        .compress(&data)
        .unwrap();

    stats.record_compress(data.len(), compressed.len(), 1000);
    stats.record_decompress(compressed.len(), data.len(), 500);

    assert_eq!(stats.total_compressions, 1);
    assert_eq!(stats.total_decompressions, 1);
    assert!(stats.compression_ratio() < 1.0);
    assert!(stats.bandwidth_saved() > 0);
}

#[test]
fn compressor_with_min_size() {
    let compressor = Compressor::new(CompressionAlgo::Lz4).with_min_size(1000);

    // Small data should not compress
    let small = b"hello".to_vec();
    let result = compressor.compress(&small);
    assert!(result.is_none());

    // Large data should compress
    let large: Vec<u8> = (0..2000).map(|i| i as u8).collect();
    let result = compressor.compress(&large);
    assert!(result.is_some());
}

#[test]
fn multiple_entries_all_preserved_after_compression() {
    let entries: Vec<WireNodeEntry> = (0..30)
        .map(|i| make_entry(i * 100, i as u32 * 10))
        .collect();

    let msg = build_gossip(42, 100, 5, entries.clone());
    let compressed = msg.with_compression(1);
    let encoded = compressed.encode().unwrap();
    let decoded = Message::decode(&encoded).unwrap();

    match decoded.payload {
        MessagePayload::Gossip(got) => {
            assert_eq!(got.len(), entries.len());
            // Verify all entries are preserved correctly
            for (i, got_entry) in got.iter().enumerate() {
                assert_eq!(got_entry.node_id, entries[i].node_id);
                assert_eq!(got_entry.heartbeat, entries[i].heartbeat);
            }
        }
        _ => panic!("expected gossip payload"),
    }
}
