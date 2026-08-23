//! DataFusion TableProvider over SysML machine HDF5 (`Machine/GpuMetric[0]/Values`).
//!
//! I/O uses **hdf5-pure** (no libhdf5 C). Scan plans partitions; execute() reads
//! K20 series-row windows. Warehouse SQL remains Impala; this is Rust SemDF parity.

mod exec;
mod provider;
mod read;

pub use provider::Hdf5TableProvider;
pub use read::{open_values, series_block_rows, SERIES_BLOCK_TARGET_BYTES};

pub const GURU_HDF5DF: &str = "#SL.00000018.HDF5DF";
pub const VALUES_PATH: &str = "Machine/GpuMetric[0]/Values";
pub const MACHINE_GROUP: &str = "Machine";
