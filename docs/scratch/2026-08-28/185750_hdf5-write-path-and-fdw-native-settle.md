# HDF5 write path: unify the `/signal` layout on jhdf, then make the tier-settle FDW-native

Date: 2026-08-28
Status: plan / proposal
Guru: `#SL.00000019.HDF5WRITE` (the reserved write seam), `#SL.00000029.HDF5SIGNAL` (the `/signal` v2 layout)
Peers: **semantics** owns the HDF5 Iceberg format contract (Python · JVM · Rust); **impala_fdw** + the **Impala fork** are the warehouse engine that consumes it; **signals** (`signal_settle.py`) is the orchestrator. This doc is a semantics-first roadmap with the consumer seams named.

## Problem

The `/signal` v2 on-disk layout has **three independent implementations and no shared source of truth**:

- **Writer — Python/h5py**: `python/hdf5_iceberg/src/hdf5_iceberg/signal_layout.py` (`write_signal_hdf5`). The *only* writer.
- **Reader — JVM/jhdf**: `java/iceberg-hdf5/.../hdf5/Hdf5SignalReader.java` + `iceberg/Hdf5ReadBuilder.java`. Consumed by the Impala fork's `IcebergHdf5Scanner` (FE) / `hdfs-hdf5-scanner.cc` (BE).
- **Reader — Rust/DataFusion**: `crates/hdf5-df/src/{read,provider,exec}.rs` (read-only; still on the legacy `/Machine` layout).

They must stay bit-compatible by hand, and they did **not**: the writer collapsed multi-GPU lanes (keyed by `series_id` alone) that the jhdf reader read correctly — the writer had to independently re-derive the `(series_id, gpu, inst)` lane identity the Kudu PK and the reader already encode (`signal_layout.py:35-40,66-68`; fixed 2026-08-28, semantics `36d5836`). That bug is the structural hazard of N hand-maintained copies of one format, not a one-off.

Downstream, this forces the **signals tier-settle to be cross-language glue** rather than SQL through the one FDW surface: `signal_settle.py` reads tier0 (psql) → **h5py** writes the file → **boto3** uploads to RustFS → a **Java subprocess** (`scripts/IcebergHdf5Register.java`) appends the Iceberg snapshot → **impyla** counts to verify → psql drops the Kudu range. Five tools, three languages.

## Why it is this way (audit, 2026-08-28)

The FDW-native settle — `INSERT INTO signals_dataproducts.signal_tier1 SELECT * FROM signals_dataproducts.signal_tier0 WHERE epoch_hour = H` — is **not available**: the Impala fork is read-only for HDF5 at every layer, so this is a legitimate bridge, not a bypass.

- FE insert analyzer: `HdfsTableSink.SUPPORTED_FILE_FORMATS` omits HDF5 (Parquet/Text/RC/Seq/Avro/Iceberg only); `InsertStmt.java:592` throws `not supported to write: 'HDF5'`.
- BE table sink: `table-sink-base.cc:240-264` builds only Text/Parquet writers; HDF5 → `default` → "Impala only supports writing to TEXT and PARQUET". `be/src/exec/hdf5/` holds only `hdfs-hdf5-scanner.{cc,h}` — no writer sibling.
- Iceberg object model: `Hdf5GenericFormatModel.writeBuilder()` is a **deliberate throwing stub** (`#SL.00000019.HDF5WRITE`), locked by `Hdf5FormatModelTest.writeBuilderIsGuruNotSilent()`. The `Hdf5OutputFormat`/`Hdf5SerDe` names in `HdfsFileFormat.java:92` are dead identifier strings (the classes do not exist).
- FDW: `impalaIsForeignRelUpdatable` marks `access 'impala_sql'` tables **non-updatable** — only `kudu_scan` (tier0) accepts INSERT (the ingest path). `impala_fdw_exec` is fenced to `ADD/DROP RANGE PARTITION` + `CREATE VIEW`. So Postgres rejects an INSERT into `signal_tier1` before Impala sees it.
- Design intent: docs describe the tier1 writer as the Python analog script (`214236_schema-final-e2e.md:417` "Generalise the tier1 writer"); `impala_fdw.md` lists **"DML in v0"** as a non-goal; the only interim framing ("Until Impala is rebuilt, DataFusion still reads the analog") is about *reading*. No doc proposes INSERT..SELECT. The h5py writer is de-facto permanent; the sole marker of a future native writer is the `#SL.00000019.HDF5WRITE` placeholder.

## Objective

1. Make the `/signal` v2 layout a **single versioned contract** so the bindings cannot diverge (the format is semantics' protocol, peer to signals-protocol).
2. Provide a **JVM (jhdf) writer** at the reserved `writeBuilder` seam, making the JVM binding write+read symmetric and letting the settle write via jhdf — retiring the second (h5py) codebase from the hot path.
3. Enable the **FDW-native settle** (`INSERT INTO tier1 SELECT FROM tier0`) by building the Impala HDF5 write sink on top of the jhdf writer, retiring the h5py/boto3/register/verify stack.

Each objective is independently valuable; they are staged so the divergence-bug class is killed in Phase 1 (contained, no C++), well before the larger Impala-fork work.

## Plan

### Phase 0 — pin the layout as one contract (semantics; small)

Extract the `/signal` v2 layout into a **single normative spec** the three bindings consume instead of re-encoding by hand:
- Today the constants live in two places: reader `Hdf5SignalReader.ROOT="/signal"`, `SCHEMA_VERSION=2`, the `/int|/dec` planes, `ts/series_id/src/gpu/gpu_null/inst/inst_null/values/present` datasets, `values @scale`, `present` semantics, and the **lane identity `(series_id, gpu, inst)`**; and again in `signal_layout.py`.
- Deliverable: a versioned `SIGNAL_V2` layout descriptor (a small shared module + a doc section here) that names every dataset, dtype, chunking (`(1, min(3600,T))`, shuffle+gzip), the unscaled-decimal `@scale`, and the lane-key rule. Reader and writer read the field names/shapes from it; a conformance test asserts a fixture matches the descriptor. This makes "writer vs reader drift" a test failure, not a production bug — the guarantee we lacked.

### Phase 1 — jhdf writer at the `writeBuilder` seam (semantics; contained, high-leverage)

Implement `Hdf5GenericFormatModel.writeBuilder` (the throwing stub) as a real jhdf `Hdf5SignalWriter`:
- Writes the `/signal` v2 layout with jhdf (`io.jhdf`), **sharing the layout descriptor + lane logic with `Hdf5SignalReader`** (one JVM codebase for write and read).
- Emits Iceberg `DataFile` metrics (the `epoch_hour`/`ts_ns`/`series_id` bounds `IcebergHdf5Register.java` attaches via `withMetrics`) so a snapshot commit carries manifest bounds.
- Expose a thin JVM entry point (a `main`, like `IcebergHdf5Register`) so `signals/scripts/signal_settle.py` calls the **jhdf** writer instead of `hdf5_iceberg.signal_layout.write_signal_hdf5`.
- Tests: (a) round-trip jhdf-write → jhdf-read → exact rows incl. multi-GPU lanes; (b) jhdf-write → **Impala** read through the fork; (c) a transitional bit-compat check jhdf-write ≡ h5py-write for the same input, retired once jhdf is authoritative.

Result: the `/signal` write+read is **one jhdf codebase**; the Python h5py writer demotes to a fixture/probe generator (or is regenerated from the descriptor), and the divergence-bug class is gone. The settle still scripts (boto3 upload + register + verify), but no longer maintains a second format implementation. **No Impala C++/BE change in this phase.**

### Phase 2 — Impala HDF5 write sink (Impala fork; larger)

Build the engine write path on the Phase-1 jhdf writer:
1. BE `HdfsHdf5TableWriter` added to the `table-sink-base.cc:240` switch — the write mirror of `hdfs-hdf5-scanner.cc`, JNI-bridged to the jhdf `Hdf5SignalWriter`.
2. Iceberg appender: the same `writeBuilder` now returns a real `DataWriteBuilder`, so `IcebergCatalogOpExecutor` can commit an HDF5 snapshot in-engine (replaces the out-of-process `IcebergHdf5Register.java`).
3. FE enablement: add `HdfsFileFormat.HDF5` to `HdfsTableSink.SUPPORTED_FILE_FORMATS`; route `FeIcebergTable.getWriteFileFormat()` (`write.format.default=hdf5`) to the new sink instead of `HdfsParquetTableWriter`; drop the `InsertStmt` rejection for HDF5.

Now `INSERT INTO signal_tier1 SELECT * FROM signal_tier0 WHERE epoch_hour=H` writes a conformant `/signal` file to the object store and commits its snapshot, in one statement.

### Phase 3 — FDW-native settle (signals; small once Phase 2 lands)

- FDW write plumbing: make `impala_sql` foreign tables updatable (push `INSERT..SELECT` to HS2 in `impalaExecForeignInsert`), or run the settle as a direct HS2 statement.
- `signal_settle.py` collapses to: `INSERT INTO signal_tier1 SELECT … WHERE epoch_hour=H` (write + commit, atomic) → optional count-verify → `DROP RANGE PARTITION`. **Retires** `write_h5`/h5py, `upload`/boto3, `iceberg_hdf5_register.sh`/`IcebergHdf5Register.java`, and `impala_count`/impyla.

## What remains regardless (policy, not format plumbing)

- **Kudu range DROP** — already FDW/Impala-native via `impala_fdw_exec('… DROP RANGE PARTITION …')`, incl. the HMS-free "range is gone despite the metadata-reload fault" handling (signals is HMS-free; the Kudu drop lands before the HMS reload fails).
- **State ledger** `signal_settle_state` (rows/registered/verified/dropped) and the drop gate (present-hours verified before DROP).
- **Schedule / orchestration** — the gaius pg_cron → `scheduled_tasks(tier_settle)` → `tier_settle` spawn (see gaius `88ecf67`). Product-generic; unaffected by the write-path work.

## Peer seams (who owns what)

- **semantics**: the `/signal` v2 contract (Phase 0) and the jhdf writer at `writeBuilder` (Phase 1). JNI-callable, mirroring how the reader is consumed. This is the deliverable that makes semantics a true peer — it *provides* the format for both directions, not just read.
- **Impala fork**: consumes the jhdf writer via JNI in a BE sink (Phase 2), exactly as it consumes `Hdf5SignalReader` via `IcebergHdf5Scanner` today.
- **signals**: consumes via SQL (Phase 3); keeps the ledger, schedule, and Kudu DROP.

## Sequencing & effort

- Phase 0 + Phase 1 are **contained to semantics (JVM + a shared descriptor), days of work, no C++**, and deliver the biggest correctness win (one format codebase). Do these first even if Phase 2 is deferred.
- Phase 2 is the Impala-fork write path — the bulk of the effort (BE writer + JNI + FE + Iceberg commit), gated on Phase 1's jhdf writer existing.
- Phase 3 is small once Phase 2 lands.

## Open questions / risks

- **jhdf write maturity**: confirm `io.jhdf` writes chunked + shuffle+gzip datasets Impala's scanner and the Rust reader both accept (jhdf has historically been read-stronger than write). Fixture cross-read (jhdf-write → h5py-read → rust-read → Impala-read) gates Phase 1.
- **Rust/DataFusion binding** is still on the legacy `/Machine` layout; either bring it onto the Phase-0 `/signal` contract or scope it out explicitly.
- **Iceberg commit semantics** for a custom (HDF5) file format via the fork's `IcebergCatalogOpExecutor` — verify snapshot/manifest behavior matches the current `IcebergHdf5Register` append (partition overwrite idempotency included).
- **HMS-free deployment**: the Impala INSERT/commit path must not depend on a live Hive Metastore any more than the current Kudu DDL does; validate against the HMS-free catalog (`signals_catalog`).
