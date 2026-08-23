use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::exec::Hdf5Exec;
use crate::read::{open_values, partitions, value_schema};

#[derive(Debug, Clone)]
pub struct Hdf5TableProvider {
    path: PathBuf,
    schema: SchemaRef,
    n_gpu: u64,
    n_time: u64,
}

impl Hdf5TableProvider {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self, DataFusionError> {
        let path = path.as_ref().to_path_buf();
        let (_file, n_gpu, n_time) = open_values(&path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            path,
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
        let parts = partitions(self.n_gpu, self.n_time);
        Ok(Arc::new(Hdf5Exec::try_new(
            self.path.clone(),
            Arc::clone(&self.schema),
            projection.cloned(),
            parts,
            self.n_time,
        )))
    }
}
