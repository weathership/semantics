use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::exec::{FileSlice, Hdf5Exec};
use crate::read::{open_values, partitions, value_schema};

#[derive(Debug, Clone)]
pub struct Hdf5TableProvider {
    files: Vec<PathBuf>,
    schema: SchemaRef,
    n_gpu: u64,
    n_time: u64,
}

impl Hdf5TableProvider {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self, DataFusionError> {
        Self::try_listing([path.as_ref().to_path_buf()])
    }

    /// One partition per (file × K20 series-block). DataFusion runs those in parallel.
    pub fn try_listing(
        files: impl IntoIterator<Item = PathBuf>,
    ) -> Result<Self, DataFusionError> {
        let files: Vec<PathBuf> = files.into_iter().collect();
        if files.is_empty() {
            return Err(DataFusionError::Plan("hdf5 listing is empty".into()));
        }
        let (_file, n_gpu, n_time) = open_values(&files[0])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for p in files.iter().skip(1) {
            let (_, g, t) = open_values(p)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if g != n_gpu || t != n_time {
                return Err(DataFusionError::Plan(format!(
                    "shape mismatch {} vs first file {n_gpu}×{n_time}",
                    p.display()
                )));
            }
        }
        Ok(Self {
            files,
            schema: value_schema(),
            n_gpu,
            n_time,
        })
    }

    pub fn n_gpu(&self) -> u64 {
        self.n_gpu
    }

    pub fn n_time(&self) -> u64 {
        self.n_time
    }

    pub fn n_files(&self) -> usize {
        self.files.len()
    }

    fn slices(&self) -> Vec<FileSlice> {
        let mut out = Vec::new();
        for path in &self.files {
            for (start_gpu, n_gpu) in partitions(self.n_gpu, self.n_time) {
                out.push(FileSlice {
                    path: path.clone(),
                    start_gpu,
                    n_gpu,
                    n_time: self.n_time,
                });
            }
        }
        out
    }
}

#[async_trait]
impl TableProvider for Hdf5TableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Hdf5Exec::try_new(
            Arc::clone(&self.schema),
            projection.cloned(),
            self.slices(),
        )))
    }
}
