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

#[derive(Debug, Clone)]
pub struct Hdf5Exec {
    path: PathBuf,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    /// (start_gpu, n_gpu) per partition
    partitions: Vec<(u64, u64)>,
    n_time: u64,
    properties: PlanProperties,
}

impl Hdf5Exec {
    pub fn try_new(
        path: PathBuf,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        partitions: Vec<(u64, u64)>,
        n_time: u64,
    ) -> Self {
        let projected = match &projection {
            Some(idx) => Arc::new(schema.project(idx).expect("projection")),
            None => Arc::clone(&schema),
        };
        let npart = partitions.len().max(1);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected)),
            Partitioning::UnknownPartitioning(npart),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            path,
            schema: projected,
            projection,
            partitions,
            n_time,
            properties,
        }
    }
}

impl DisplayAs for Hdf5Exec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Hdf5Exec: file={}, partitions={}",
            self.path.display(),
            self.partitions.len()
        )
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
        let (start, n) = self.partitions.get(partition).copied().ok_or_else(|| {
            DataFusionError::Internal(format!("partition {partition} out of range"))
        })?;
        let path = self.path.clone();
        let n_time = self.n_time;
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let batch = read_partition(path.as_path(), start, n, n_time, projection.as_deref())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }
}

