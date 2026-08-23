//! Many-file HDF5 scan bench (DataFusion + hdf5-pure).
//!
//! Local disk or Signals RustFS (`s3://signals-dataproducts/iceberg/...`,
//! path-style :9010). RustFS mode **fails** if the S3 API is down
//! (`#SL.00000020.NORUSTFS`) — no skip.

use std::fs;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use datafusion::prelude::SessionContext;
use hdf5_df::{Hdf5TableProvider, GURU_NORUSTFS};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use tokio::runtime::Runtime;

#[derive(Clone, ValueEnum)]
enum Target {
    Local,
    Rustfs,
}

#[derive(Parser)]
#[command(name = "hdf5-df-bench", about = "Parallel hdf5-pure + DataFusion vs many HDF5 objects")]
struct Args {
    /// Analog fixture to replicate
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long, default_value_t = 32)]
    n_files: usize,
    #[arg(long, value_enum, default_value_t = Target::Local)]
    target: Target,
    #[arg(long, default_value = "127.0.0.1:9010")]
    rustfs_addr: String,
    #[arg(long, default_value = "signals-dataproducts")]
    bucket: String,
    #[arg(long, default_value = "iceberg/bench/hdf5-df")]
    prefix: String,
    #[arg(long, env = "RUSTFS_ACCESS_KEY", default_value = "rustfsadmin")]
    access: String,
    #[arg(long, env = "RUSTFS_SECRET_KEY", default_value = "rustfsadmin")]
    secret: String,
    #[arg(long)]
    work_dir: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();
    if let Err(e) = run(args) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    if !args.fixture.is_file() {
        return Err(format!("fixture missing: {}", args.fixture.display()).into());
    }
    let bytes = fs::read(&args.fixture)?;
    let work = args.work_dir.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("hdf5-df-bench-{}", std::process::id()))
    });
    fs::create_dir_all(&work)?;
    let mut files = Vec::with_capacity(args.n_files);
    for i in 0..args.n_files {
        let p = work.join(format!("part-{i:04}.h5"));
        fs::write(&p, &bytes)?;
        files.push(p);
    }

    if matches!(args.target, Target::Rustfs) {
        if TcpStream::connect(&args.rustfs_addr).is_err() {
            return Err(format!(
                "{GURU_NORUSTFS} RustFS S3 API not listening on {} (path-style; lab :9010)",
                args.rustfs_addr
            )
            .into());
        }
        let rt = Runtime::new()?;
        rt.block_on(push_rustfs(&args, &files))?;
        let fetched = work.join("from-rustfs");
        fs::create_dir_all(&fetched)?;
        rt.block_on(pull_rustfs(&args, &fetched, args.n_files))?;
        files = (0..args.n_files)
            .map(|i| fetched.join(format!("part-{i:04}.h5")))
            .collect();
    }

    let t0 = Instant::now();
    let provider = Hdf5TableProvider::try_listing(files.clone())?;
    let n_files = provider.n_files();
    let expect = (provider.n_gpu() * provider.n_time() * n_files as u64) as i64;
    let ctx = SessionContext::new();
    ctx.register_table("gpu_power", Arc::new(provider))?;
    let rt = Runtime::new()?;
    let n = rt.block_on(async {
        let df = ctx.sql("SELECT COUNT(*) AS n FROM gpu_power").await?;
        let batches = df.collect().await?;
        let col = batches[0].column(0);
        Ok::<i64, datafusion::error::DataFusionError>(count_i64(col))
    })?;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    if n != expect {
        return Err(format!("COUNT {n} != {expect}").into());
    }
    let bytes_total = bytes.len() as f64 * args.n_files as f64;
    println!(
        r#"{{"engine":"hdf5-df","target":"{}","files":{},"rows":{},"ms":{:.3},"MBps":{:.3}}}"#,
        match args.target {
            Target::Local => "local",
            Target::Rustfs => "rustfs",
        },
        args.n_files,
        n,
        ms,
        (bytes_total / 1_000_000.0) / (ms / 1000.0)
    );
    Ok(())
}

fn count_i64(col: &dyn datafusion::arrow::array::Array) -> i64 {
    use datafusion::arrow::array::{Int64Array, UInt64Array};
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        a.value(0)
    } else {
        col.as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count")
            .value(0) as i64
    }
}

async fn push_rustfs(args: &Args, files: &[PathBuf]) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = if args.rustfs_addr.starts_with("http") {
        args.rustfs_addr.clone()
    } else {
        format!("http://{}", args.rustfs_addr)
    };
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(&args.bucket)
        .with_access_key_id(&args.access)
        .with_secret_access_key(&args.secret)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;
    for (i, p) in files.iter().enumerate() {
        let key = ObjPath::from(format!("{}/part-{i:04}.h5", args.prefix.trim_matches('/')));
        let b = fs::read(p)?;
        store.put(&key, PutPayload::from(b)).await?;
    }
    eprintln!(
        "uploaded {} objects s3://{}/{}",
        files.len(),
        args.bucket,
        args.prefix
    );
    Ok(())
}

async fn pull_rustfs(
    args: &Args,
    dest: &std::path::Path,
    n: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = if args.rustfs_addr.starts_with("http") {
        args.rustfs_addr.clone()
    } else {
        format!("http://{}", args.rustfs_addr)
    };
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(&args.bucket)
        .with_access_key_id(&args.access)
        .with_secret_access_key(&args.secret)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()?;
    for i in 0..n {
        let key = ObjPath::from(format!("{}/part-{i:04}.h5", args.prefix.trim_matches('/')));
        let get = store.get(&key).await?;
        let b = get.bytes().await?;
        fs::write(dest.join(format!("part-{i:04}.h5")), b)?;
    }
    Ok(())
}

