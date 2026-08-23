use std::any::Any;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::stream;

use crate::read::read_partition;

/// One DataFusion partition: one HDF5 file + K20 row window.
#[derive(Debug, Clone)]
pub struct FileSlice {
    pub path: PathBuf,
    pub start_gpu: u64,
    pub n_gpu: u64,
    pub n_time: u64,
}

#[derive(Debug, Clone)]
pub struct Hdf5Exec {
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    slices: Vec<FileSlice>,
    properties: PlanProperties,
}

impl Hdf5Exec {
    pub fn try_new(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        slices: Vec<FileSlice>,
    ) -> Self {
        let projected = match &projection {
            Some(idx) => Arc::new(schema.project(idx).expect("projection")),
            None => Arc::clone(&schema),
        };
        let npart = slices.len().max(1);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected)),
            Partitioning::UnknownPartitioning(npart),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            schema: projected,
            projection,
            slices,
            properties,
        }
    }
}

impl DisplayAs for Hdf5Exec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Hdf5Exec: partitions={}", self.slices.len())
    }
}

impl Display for Hdf5Exec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlan for Hdf5Exec {
    fn name(&self) -> &str {
        "Hdf5Exec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "Hdf5Exec does not have children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let slice = self.slices.get(partition).cloned().ok_or_else(|| {
            DataFusionError::Internal(format!("partition {partition} out of range"))
        })?;
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let batch = read_partition(
            slice.path.as_path(),
            slice.start_gpu,
            slice.n_gpu,
            slice.n_time,
            projection.as_deref(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }
}
