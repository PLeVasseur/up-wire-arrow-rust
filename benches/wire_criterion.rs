// SPDX-License-Identifier: Apache-2.0

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use up_rust::{DecodePayload, EncodePayload, ReadDecodePayload};
use up_wire_arrow::{ArrowWire, TelemetryTableV1, ARROW_IPC_DECODE_LIMIT};

const ROW_COUNTS: [usize; 3] = [64, 4_096, 65_536];

fn owned_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/owned_encode_one_serialization");
    for rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));
        let table = TelemetryTableV1::fixture(rows, 7);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &table, |b, value| {
            b.iter(|| {
                <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload_owned(black_box(
                    value,
                ))
                .expect("owned encode")
            });
        });
    }
    group.finish();
}

fn layout_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/layout_probe_one_serialization");
    for rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));
        let table = TelemetryTableV1::fixture(rows, 7);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &table, |b, value| {
            b.iter(|| {
                <ArrowWire as EncodePayload<TelemetryTableV1>>::payload_layout(black_box(value))
                    .expect("layout probe")
            });
        });
    }
    group.finish();
}

fn direct_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/direct_encode_one_serialization_plus_copy");
    for rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));
        let table = TelemetryTableV1::fixture(rows, 7);
        let encoded = <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload_owned(&table)
            .expect("fixture encode");
        let mut destination = vec![0_u8; encoded.len()];
        group.bench_function(BenchmarkId::from_parameter(rows), |b| {
            b.iter(|| {
                <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload(
                    black_box(&table),
                    black_box(&mut destination),
                )
                .expect("direct encode");
            });
        });
    }
    group.finish();
}

fn contiguous_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/contiguous_decode");
    for rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));
        let table = TelemetryTableV1::fixture(rows, 7);
        let encoded = <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload_owned(&table)
            .expect("fixture encode");
        group.bench_function(BenchmarkId::from_parameter(rows), |b| {
            b.iter(|| {
                <ArrowWire as DecodePayload<'_, TelemetryTableV1>>::decode_payload(black_box(
                    &encoded,
                ))
                .expect("contiguous decode")
            });
        });
    }
    group.finish();
}

fn reader_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/reader_decode_with_exact_copy");
    for rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));
        let table = TelemetryTableV1::fixture(rows, 7);
        let encoded = <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload_owned(&table)
            .expect("fixture encode");
        group.bench_function(BenchmarkId::from_parameter(rows), |b| {
            b.iter(|| {
                <ArrowWire as ReadDecodePayload<TelemetryTableV1>>::decode_payload_from_reader(
                    black_box(encoded.as_ref()),
                    encoded.len(),
                    ARROW_IPC_DECODE_LIMIT,
                )
                .expect("reader decode")
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    owned_encode,
    layout_probe,
    direct_encode,
    contiguous_decode,
    reader_decode
);
criterion_main!(benches);
