use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int16Array, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use hdf5_pure::{AttrValue, File};

use crate::{GURU_HDF5DF, MACHINE_GROUP, VALUES_PATH};
use semdf::{
    AGGREGATIONS, GRAIN, MEASURE_IRI, QUANTITY_KIND, UNIT,
};

pub const SERIES_BLOCK_TARGET_BYTES: u64 = 8 * 1024 * 1024;

pub fn series_block_rows(n_time: u64) -> u64 {
    let row_bytes = n_time.saturating_mul(2).max(1);
    (SERIES_BLOCK_TARGET_BYTES / row_bytes).max(1)
}

#[derive(Debug, thiserror::Error)]
pub enum Hdf5DfError {
    #[error("{code} {message}")]
    Guru { code: &'static str, message: String },
}

impl Hdf5DfError {
    pub fn guru(message: impl Into<String>) -> Self {
        Self::Guru {
            code: GURU_HDF5DF,
            message: message.into(),
        }
    }
}

fn attr_string(v: &AttrValue) -> Option<String> {
    match v {
        AttrValue::String(s) | AttrValue::AsciiString(s) => {
            Some(s.trim_end_matches('\0').to_string())
        }
        _ => None,
    }
}

/// Open analog Values; return (n_gpu, n_time). Fail `#SL.00000018` if missing.
pub fn open_values(path: &Path) -> Result<(File, u64, u64), Hdf5DfError> {
    let file = File::open(path).map_err(|e| Hdf5DfError::guru(format!("open: {e}")))?;
    let _machine = file
        .group(MACHINE_GROUP)
        .map_err(|e| Hdf5DfError::guru(format!("Machine group: {e}")))?;
    let ds = file
        .dataset(VALUES_PATH)
        .map_err(|e| Hdf5DfError::guru(format!("{VALUES_PATH}: {e}")))?;
    let shape = ds
        .shape()
        .map_err(|e| Hdf5DfError::guru(format!("shape: {e}")))?;
    if shape.len() != 2 {
        return Err(Hdf5DfError::guru(format!(
            "Values rank {} want 2",
            shape.len()
        )));
    }
    Ok((file, shape[0], shape[1]))
}

#[allow(dead_code)]
pub fn machine_see_also(path: &Path) -> Result<String, Hdf5DfError> {
    let file = File::open(path).map_err(|e| Hdf5DfError::guru(format!("open: {e}")))?;
    let g = file
        .group(MACHINE_GROUP)
        .map_err(|e| Hdf5DfError::guru(format!("Machine: {e}")))?;
    let attrs = g
        .attrs()
        .map_err(|e| Hdf5DfError::guru(format!("Machine attrs: {e}")))?;
    attrs
        .get("rdfs.seeAlso")
        .and_then(attr_string)
        .ok_or_else(|| Hdf5DfError::guru("Machine missing rdfs.seeAlso"))
}

pub fn value_schema() -> SchemaRef {
    let mut meta = HashMap::new();
    meta.insert(
        MEASURE_IRI.to_string(),
        "https://signals.zndx.org/sdg#GpuPower".to_string(),
    );
    meta.insert(UNIT.to_string(), "W".to_string());
    meta.insert(QUANTITY_KIND.to_string(), "intensive".to_string());
    meta.insert(GRAIN.to_string(), "gpu×time".to_string());
    meta.insert(
        AGGREGATIONS.to_string(),
        r#"["AVG","MIN","MAX","COUNT"]"#.to_string(),
    );
    let value = Field::new("value", DataType::Int16, false).with_metadata(meta);
    Arc::new(Schema::new(vec![
        Field::new("gpu_index", DataType::UInt32, false),
        Field::new("sample", DataType::UInt32, false),
        value,
    ]))
}

pub fn partitions(n_gpu: u64, n_time: u64) -> Vec<(u64, u64)> {
    let block = series_block_rows(n_time);
    let mut out = Vec::new();
    let mut start = 0u64;
    while start < n_gpu {
        let n = (n_gpu - start).min(block);
        out.push((start, n));
        start += n;
    }
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

pub fn read_partition(
    path: &Path,
    start_gpu: u64,
    n_gpu: u64,
    n_time: u64,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, Hdf5DfError> {
    let file = File::open(path).map_err(|e| Hdf5DfError::guru(format!("open: {e}")))?;
    let ds = file
        .dataset(VALUES_PATH)
        .map_err(|e| Hdf5DfError::guru(format!("{VALUES_PATH}: {e}")))?;
    let flat = ds
        .read_i16_rows(start_gpu, n_gpu)
        .map_err(|e| Hdf5DfError::guru(format!("read_i16_rows: {e}")))?;
    let nrows = (n_gpu * n_time) as usize;
    if flat.len() != nrows {
        return Err(Hdf5DfError::guru(format!(
            "got {} i16 want {nrows} ({n_gpu}×{n_time})",
            flat.len()
        )));
    }
    let mut gpu_index = Vec::with_capacity(nrows);
    let mut sample = Vec::with_capacity(nrows);
    for g in 0..n_gpu {
        for t in 0..n_time {
            gpu_index.push((start_gpu + g) as u32);
            sample.push(t as u32);
        }
    }
    let schema = value_schema();
    let arrays: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt32Array::from(gpu_index)),
        Arc::new(UInt32Array::from(sample)),
        Arc::new(Int16Array::from(flat)),
    ];
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| Hdf5DfError::guru(format!("record batch: {e}")))?;
    match projection {
        None => Ok(batch),
        Some(idx) => batch
            .project(idx)
            .map_err(|e| Hdf5DfError::guru(format!("project: {e}"))),
    }
}
