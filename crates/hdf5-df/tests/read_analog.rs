use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, UInt64Array};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use hdf5_df::{series_block_rows, Hdf5TableProvider, GURU_HDF5DF};
use semdf::{check_aggregation, MEASURE_IRI};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../analog/fixtures/sdg_machine_small.h5")
}

#[test]
fn k20_formula() {
    assert_eq!(series_block_rows(10000), 419);
}

#[tokio::test]
async fn scan_analog_count_and_semdf() {
    let path = fixture();
    assert!(path.exists(), "{}", path.display());
    let provider = Hdf5TableProvider::try_new(&path).expect("open analog");
    assert_eq!(provider.n_gpu(), 8);
    assert_eq!(provider.n_time(), 64);
    let schema = provider.schema();
    let value = schema.field_with_name("value").unwrap();
    let meta = value.metadata();
    assert!(
        meta.get(MEASURE_IRI)
            .is_some_and(|v| v.ends_with("GpuPower")),
        "{meta:?}"
    );
    let fmap: HashMap<String, String> = meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    check_aggregation(Some(&fmap), "AVG").unwrap();
    let err = check_aggregation(Some(&fmap), "SUM").unwrap_err();
    assert!(err.to_string().contains("#SL.00000002"));

    let ctx = SessionContext::new();
    ctx.register_table("gpu_power", Arc::new(provider))
        .unwrap();
    let df = ctx.sql("SELECT COUNT(*) AS n FROM gpu_power").await.unwrap();
    let batches = df.collect().await.unwrap();
    let col = batches[0].column(0);
    let n = if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        a.value(0)
    } else {
        col.as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count type")
            .value(0) as i64
    };
    assert_eq!(n, 8 * 64);
}

#[test]
fn missing_file_is_guru() {
    let err = Hdf5TableProvider::try_new("/no/such/machine.h5").unwrap_err();
    assert!(
        err.to_string().contains(GURU_HDF5DF),
        "{err}"
    );
}
