//! Benchmarks for the protocol module.
//!
//! Run with: `cargo bench --bench protocol_bench`

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pgwire_replication::lsn::Lsn;
use pgwire_replication::protocol::messages::{parse_error_response, ErrorFields};
use pgwire_replication::protocol::replication::{encode_standby_status_update, parse_copy_data};

/// Generate a realistic XLogData payload
fn make_xlogdata_payload(data_size: usize) -> Bytes {
    let mut v = Vec::with_capacity(1 + 24 + data_size);
    v.push(b'w');
    v.extend_from_slice(&0x0123456789ABCDEFu64.to_be_bytes()); // wal_start
    v.extend_from_slice(&0xFEDCBA9876543210u64.to_be_bytes()); // wal_end
    v.extend_from_slice(&1234567890i64.to_be_bytes()); // server_time
    v.extend_from_slice(&vec![0x42u8; data_size]); // payload
    Bytes::from(v)
}

/// Generate a KeepAlive payload
fn make_keepalive_payload() -> Bytes {
    let mut v = Vec::with_capacity(18);
    v.push(b'k');
    v.extend_from_slice(&100i64.to_be_bytes());
    v.extend_from_slice(&200i64.to_be_bytes());
    v.push(1);
    Bytes::from(v)
}

/// Generate a realistic error response payload
fn make_error_payload() -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"SERROR\0");
    payload.extend_from_slice(b"VFATAL\0");
    payload.extend_from_slice(b"C42P01\0");
    payload.extend_from_slice(b"Mrelation \"users\" does not exist\0");
    payload.extend_from_slice(b"Dtable was dropped in a previous migration\0");
    payload.extend_from_slice(b"Hcheck your migration scripts\0");
    payload.extend_from_slice(b"Fparse_relation.c\0");
    payload.extend_from_slice(b"L1234\0");
    payload.extend_from_slice(b"Rparseropen\0");
    payload.push(0);
    payload
}

fn bench_parse_xlogdata(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_xlogdata");

    for size in [64, 256, 1024, 4096, 16384] {
        let payload = make_xlogdata_payload(size);
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.iter(|| parse_copy_data(black_box(payload.clone())));
        });
    }

    group.finish();
}

fn bench_parse_keepalive(c: &mut Criterion) {
    let payload = make_keepalive_payload();

    c.bench_function("parse_keepalive", |b| {
        b.iter(|| parse_copy_data(black_box(payload.clone())));
    });
}

fn bench_encode_status_update(c: &mut Criterion) {
    c.bench_function("encode_standby_status_update", |b| {
        b.iter(|| {
            encode_standby_status_update(
                black_box(Lsn(0x123456789ABCDEF0)),
                black_box(1234567890),
                black_box(false),
            )
        });
    });
}

fn bench_parse_error_response(c: &mut Criterion) {
    let payload = make_error_payload();

    c.bench_function("parse_error_response", |b| {
        b.iter(|| parse_error_response(black_box(&payload)));
    });
}

fn bench_error_fields_parse(c: &mut Criterion) {
    let payload = make_error_payload();

    c.bench_function("ErrorFields::parse", |b| {
        b.iter(|| ErrorFields::parse(black_box(&payload)));
    });
}

criterion_group!(
    benches,
    bench_parse_xlogdata,
    bench_parse_keepalive,
    bench_encode_status_update,
    bench_parse_error_response,
    bench_error_fields_parse,
);
criterion_main!(benches);
