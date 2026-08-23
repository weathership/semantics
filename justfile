# Semantic layer CI. Not a smoke. Elevated gate: semantic-ci.
# Analog tests generate a small .h5 in tmp (7/5/56). Do not prepend /usr/lib to
# LD_LIBRARY_PATH (Nix glibc). Run `just` from a devenv/nix shell so numpy finds zlib.

root := justfile_directory()

default: check test

check:
    cargo check --manifest-path {{root}}/Cargo.toml --workspace
    cargo test --manifest-path {{root}}/Cargo.toml --workspace

test-python:
    uv run --project {{root}}/python/semdf --with pytest --with pyarrow \
        pytest -q {{root}}/python/semdf/tests
    uv run --with pytest --with h5py --with numpy \
        pytest -q {{root}}/analog/test_analog.py
    PYTHONPATH={{root}}/python/hdf5_iceberg/src:{{root}}/python/semdf/src \
        uv run --with pytest --with h5py --with numpy --with pyarrow \
        pytest -q {{root}}/python/hdf5_iceberg/tests

test: check test-python

analog OUT="analog/fixtures/sdg_machine_small.h5":
    uv run --with h5py --with numpy \
        python {{root}}/analog/generate_sdg_hdf5.py --out {{root}}/{{OUT}} --n-series 8 --n-time 64

# Elevated gate (not smoke).
semantic-ci: analog test

# Many-file scan: DataFusion+hdf5-pure vs h5py. Local copies of the analog.
# Not smoke. RustFS target: `just hdf5-df-bench-rustfs` (fails if :9010 down).
hdf5-df-bench n="32":
    #!/usr/bin/env bash
    set -euo pipefail
    WORK=$(mktemp -d)
    cargo build -p hdf5-df --bin hdf5-df-bench --quiet
    ./target/debug/hdf5-df-bench --fixture {{root}}/analog/fixtures/sdg_machine_small.h5 \
        --n-files {{n}} --target local --work-dir "$WORK"
    uv run --with h5py --with numpy python {{root}}/analog/bench_h5py.py --work-dir "$WORK" --workers 1
    uv run --with h5py --with numpy python {{root}}/analog/bench_h5py.py --work-dir "$WORK" --workers {{n}}

hdf5-df-bench-rustfs n="32":
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build -p hdf5-df --bin hdf5-df-bench --quiet
    ./target/debug/hdf5-df-bench --fixture {{root}}/analog/fixtures/sdg_machine_small.h5 \
        --n-files {{n}} --target rustfs --rustfs-addr "${RUSTFS_ADDRESS:-127.0.0.1:9010}"

