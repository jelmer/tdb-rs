use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use trivialdb::{Flags, StoreFlags, Tdb};

fn bench_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("store");

    for size in [10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();
            let value = vec![0u8; size];
            let mut counter = 0u64;

            b.iter(|| {
                let key = counter.to_be_bytes();
                tdb.store(black_box(&key), black_box(&value), None).unwrap();
                counter = counter.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn bench_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch");

    for size in [10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();
            let value = vec![0u8; size];

            // Pre-populate with 1000 entries
            for i in 0..1000u64 {
                let key = i.to_be_bytes();
                tdb.store(&key, &value, None).unwrap();
            }

            let mut counter = 0u64;
            b.iter(|| {
                let key = (counter % 1000).to_be_bytes();
                let result = tdb.fetch(black_box(&key)).unwrap();
                black_box(result);
                counter = counter.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();
                // Pre-populate with entries
                for i in 0..1000u64 {
                    let key = i.to_be_bytes();
                    tdb.store(&key, b"value", None).unwrap();
                }
                (tdb, 0u64)
            },
            |(mut tdb, counter)| {
                let key = (counter % 1000).to_be_bytes();
                tdb.delete(black_box(&key)).unwrap();
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_exists(c: &mut Criterion) {
    let mut group = c.benchmark_group("exists");

    group.bench_function("exists_check", |b| {
        let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();

        // Pre-populate with 1000 entries
        for i in 0..1000u64 {
            let key = i.to_be_bytes();
            tdb.store(&key, b"value", None).unwrap();
        }

        let mut counter = 0u64;
        b.iter(|| {
            let key = (counter % 1000).to_be_bytes();
            let result = tdb.exists(black_box(&key));
            black_box(result);
            counter = counter.wrapping_add(1);
        });
    });

    group.finish();
}

fn bench_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration");

    for count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();

            // Pre-populate
            for i in 0..count {
                let key = (i as u64).to_be_bytes();
                tdb.store(&key, b"value", None).unwrap();
            }

            b.iter(|| {
                let keys: Vec<_> = tdb.keys().collect();
                black_box(keys);
            });
        });
    }

    group.finish();
}

fn bench_iteration_with_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration_with_values");

    for count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();

            // Pre-populate
            for i in 0..count {
                let key = (i as u64).to_be_bytes();
                let value = vec![0u8; 100];
                tdb.store(&key, &value, None).unwrap();
            }

            b.iter(|| {
                let items: Vec<_> = tdb.iter().collect();
                black_box(items);
            });
        });
    }

    group.finish();
}

fn bench_store_flags(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_flags");

    group.bench_function("insert_flag", |b| {
        b.iter_batched(
            || {
                let tdb = Tdb::memory(None, Flags::empty()).unwrap();
                (tdb, 0u64)
            },
            |(mut tdb, counter)| {
                let key = counter.to_be_bytes();
                tdb.store(
                    black_box(&key),
                    black_box(b"value"),
                    Some(StoreFlags::Insert),
                )
                .unwrap();
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("replace_flag", |b| {
        b.iter_batched(
            || {
                let mut tdb = Tdb::memory(None, Flags::empty()).unwrap();
                // Pre-populate with entries
                for i in 0..1000u64 {
                    let key = i.to_be_bytes();
                    tdb.store(&key, b"old", None).unwrap();
                }
                (tdb, 0u64)
            },
            |(mut tdb, counter)| {
                let key = (counter % 1000).to_be_bytes();
                tdb.store(
                    black_box(&key),
                    black_box(b"new"),
                    Some(StoreFlags::Replace),
                )
                .unwrap();
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_store,
    bench_fetch,
    bench_delete,
    bench_exists,
    bench_iteration,
    bench_iteration_with_values,
    bench_store_flags
);
criterion_main!(benches);
