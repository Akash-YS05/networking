/// Performance benchmarks for the gossip membership protocol.
///
/// Benchmarks cover the hot paths: membership merge, message codec,
/// gossip round construction, and anti-entropy chunking.
///
/// Run with:  cargo bench
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use gossip_membership::anti_entropy;
use gossip_membership::gossip;
use gossip_membership::membership::MembershipTable;
use gossip_membership::message::{build_gossip, build_ping, status, Message, WireNodeEntry};
use gossip_membership::node::{NodeState, NodeStatus};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn addr(id: u64) -> SocketAddr {
    SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, (id >> 8) as u8, id as u8)),
        9000 + id as u16,
    )
}

fn wire_entry(id: u64) -> WireNodeEntry {
    WireNodeEntry {
        node_id: id,
        heartbeat: id as u32,
        incarnation: 0,
        status: status::ALIVE,
        addr: addr(id),
    }
}

/// Build a table with `n` peer entries (IDs 2..=n+1, local_id=1).
fn table_with_peers(n: usize) -> MembershipTable {
    let mut t = MembershipTable::new(1, addr(1));
    for i in 2..=(n as u64 + 1) {
        t.merge_entry(&NodeState::new_alive(i, addr(i), i as u32));
    }
    t
}

/// Build a vector of `n` wire entries.
fn wire_entries(n: usize) -> Vec<WireNodeEntry> {
    (1..=n as u64).map(wire_entry).collect()
}

/// Build a mixed-status set of incoming entries for merge benchmarks.
fn incoming_entries(n: usize) -> Vec<NodeState> {
    (2..=(n as u64 + 1))
        .map(|i| {
            let mut s = NodeState::new_alive(i, addr(i), (i * 2) as u32);
            s.incarnation = (i % 3) as u32;
            s.status = match i % 4 {
                0 => NodeStatus::Suspect,
                1 => NodeStatus::Dead,
                _ => NodeStatus::Alive,
            };
            s
        })
        .collect()
}

const SIZES: &[usize] = &[10, 100, 1000];

// ── Benchmarks ──────────────────────────────────────────────────────────────

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("membership_merge");
    for &n in SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("merge_digest", n), &n, |b, &n| {
            let incoming = incoming_entries(n);
            b.iter(|| {
                let mut table = table_with_peers(n);
                black_box(table.merge_digest(black_box(&incoming)));
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("membership_merge_single");
    for &n in SIZES {
        group.bench_with_input(BenchmarkId::new("merge_entry", n), &n, |b, &n| {
            // Merge a single entry into a pre-populated table.
            // We use a new node ID each time so it's always an insert (worst case).
            let entry = NodeState::new_alive(n as u64 + 100, addr(n as u64 + 100), 9999);
            b.iter(|| {
                let mut t = table_with_peers(n);
                black_box(t.merge_entry(black_box(&entry)));
            });
        });
    }
    group.finish();
}

fn bench_encode_decode(c: &mut Criterion) {
    // Encode/decode with varying entry counts (capped at 50 to fit MTU).
    let encode_sizes: &[usize] = &[0, 10, 50];

    let mut group = c.benchmark_group("message_encode");
    for &n in encode_sizes {
        let entries = wire_entries(n);
        let msg = build_gossip(1, 42, 0, entries);
        let encoded = msg.encode().unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(BenchmarkId::new("encode", n), &msg, |b, msg| {
            b.iter(|| black_box(msg.encode().unwrap()));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("message_decode");
    for &n in encode_sizes {
        let entries = wire_entries(n);
        let msg = build_gossip(1, 42, 0, entries);
        let buf = msg.encode().unwrap();

        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_with_input(BenchmarkId::new("decode", n), &buf, |b, buf| {
            b.iter(|| black_box(Message::decode(black_box(buf)).unwrap()));
        });
    }
    group.finish();

    // Ping encode/decode (header only, no entries).
    let mut group = c.benchmark_group("message_ping");
    let ping = build_ping(1, 42, 0, vec![]);
    let ping_buf = ping.encode().unwrap();
    group.bench_function("encode", |b| {
        b.iter(|| black_box(ping.encode().unwrap()));
    });
    group.bench_function("decode", |b| {
        b.iter(|| black_box(Message::decode(black_box(&ping_buf)).unwrap()));
    });
    group.finish();
}

fn bench_gossip_round(c: &mut Criterion) {
    let mut group = c.benchmark_group("gossip_round");
    for &n in SIZES {
        group.bench_with_input(BenchmarkId::new("build_gossip_message", n), &n, |b, &n| {
            let table = table_with_peers(n);
            let fanout = gossip::effective_fanout(50, table.entries.len(), true);
            b.iter(|| {
                black_box(gossip::build_gossip_message(
                    black_box(&table),
                    1,
                    42,
                    0,
                    fanout,
                ));
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("gossip_peer_selection");
    for &n in SIZES {
        group.bench_with_input(BenchmarkId::new("pick_gossip_targets", n), &n, |b, &n| {
            let table = table_with_peers(n);
            let max = gossip::effective_gossip_targets(1, table.entries.len(), true);
            b.iter(|| {
                black_box(gossip::pick_gossip_targets(black_box(&table), 1, max));
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("gossip_fanout_calc");
    for &n in SIZES {
        group.bench_with_input(BenchmarkId::new("effective_fanout", n), &n, |b, &n| {
            b.iter(|| black_box(gossip::effective_fanout(50, black_box(n), true)));
        });
    }
    group.finish();
}

fn bench_anti_entropy(c: &mut Criterion) {
    let mut group = c.benchmark_group("anti_entropy_chunk");
    for &n in SIZES {
        let entries = wire_entries(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("chunk_entries", n),
            &entries,
            |b, entries| {
                b.iter(|| black_box(anti_entropy::chunk_entries(black_box(entries))));
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("anti_entropy_build");
    for &n in SIZES {
        let entries = wire_entries(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("build_chunks", n),
            &entries,
            |b, entries| {
                b.iter(|| {
                    black_box(anti_entropy::build_chunks(black_box(entries), 1, 42, 0, 1));
                });
            },
        );
    }
    group.finish();

    // Full encode: build_chunks + encode each message.
    let mut group = c.benchmark_group("anti_entropy_encode");
    for &n in SIZES {
        let entries = wire_entries(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("build_and_encode", n),
            &entries,
            |b, entries| {
                b.iter(|| {
                    let chunks = anti_entropy::build_chunks(entries, 1, 42, 0, 1);
                    for msg in &chunks {
                        black_box(msg.encode().unwrap());
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_gossip_digest(c: &mut Criterion) {
    let mut group = c.benchmark_group("gossip_digest");
    for &n in SIZES {
        let table = table_with_peers(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("gossip_wire_entries", n),
            &table,
            |b, table| {
                b.iter(|| black_box(table.gossip_wire_entries(black_box(50))));
            },
        );
    }
    group.finish();
}

// ── Compression Benchmarks ─────────────────────────────────────────────────────

fn bench_compression(c: &mut Criterion) {
    use gossip_membership::compression::{CompressionAlgo, Compressor};

    // Test with different payload sizes
    let compress_sizes: &[usize] = &[10, 50, 100, 500];

    // Small payload (no compression expected)
    let small_data = b"This is a small test message!".to_vec();

    // Large payload (should compress well)
    let large_data: Vec<u8> = (0..1000_u32).flat_map(|i| i.to_be_bytes()).collect();

    let mut group = c.benchmark_group("compression_small");
    group.bench_function("compress", |b| {
        let compressor = Compressor::new(CompressionAlgo::Lz4);
        b.iter(|| black_box(compressor.compress(&small_data)));
    });
    group.finish();

    let mut group = c.benchmark_group("compression_large");
    group.bench_function("compress", |b| {
        let compressor = Compressor::new(CompressionAlgo::Lz4);
        b.iter(|| black_box(compressor.compress(&large_data)));
    });
    group.finish();

    // Benchmark message encoding with compression (sizes that fit in MTU)
    let message_sizes: &[usize] = &[10, 25, 40]; // Must fit in 1400 bytes after compression

    let mut group = c.benchmark_group("message_compressed_encode");
    for &n in message_sizes {
        let entries = wire_entries(n);
        let msg = build_gossip(1, 42, 0, entries);
        let compressed_msg = msg.clone().with_compression(1);
        let encoded = compressed_msg.encode().unwrap();

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("encode_compressed", n),
            &compressed_msg,
            |b, msg| {
                b.iter(|| black_box(msg.encode().unwrap()));
            },
        );
    }
    group.finish();

    // Compare compressed vs uncompressed size
    let mut group = c.benchmark_group("compression_ratio");
    for &n in message_sizes {
        let entries = wire_entries(n);
        let msg = build_gossip(1, 42, 0, entries);

        let uncompressed = msg.clone().encode().unwrap();
        let compressed = msg.with_compression(1).encode().unwrap();

        let ratio = compressed.len() as f64 / uncompressed.len() as f64;
        println!(
            "Payload size {}: {} -> {} bytes (ratio: {:.2})",
            n,
            uncompressed.len(),
            compressed.len(),
            ratio
        );

        group.bench_with_input(
            BenchmarkId::new("ratio", n),
            &(uncompressed.len(), compressed.len()),
            |b, _| {
                b.iter(|| black_box(()));
            },
        );
    }
    group.finish();

    // CPU time comparison
    let mut group = c.benchmark_group("compression_cpu_vs_bandwidth");
    for &n in message_sizes {
        let entries = wire_entries(n);
        let msg = build_gossip(1, 42, 0, entries);

        // Measure uncompressed
        let uncompressed_size = msg.encode().unwrap().len();
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = black_box(msg.clone().encode().unwrap());
        }
        let uncompressed_us = start.elapsed().as_micros() / 1000;

        // Measure compressed
        let compressed_msg = msg.with_compression(1);
        let compressed_size = compressed_msg.encode().unwrap().len();
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = black_box(compressed_msg.encode().unwrap());
        }
        let compressed_us = start.elapsed().as_micros() / 1000;

        let bandwidth_saved = uncompressed_size - compressed_size;

        println!(
            "{} entries: uncompressed={}us, compressed={}us, size_reduction={} bytes ({:.1}% saved)",
            n,
            uncompressed_us,
            compressed_us,
            bandwidth_saved,
            (bandwidth_saved as f64 / uncompressed_size as f64) * 100.0
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_merge,
    bench_encode_decode,
    bench_gossip_round,
    bench_anti_entropy,
    bench_gossip_digest,
    bench_compression,
);
criterion_main!(benches);
