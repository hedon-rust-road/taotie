use polars::{lazy::dsl::cols, prelude::*};

fn main() -> anyhow::Result<()> {
    let df = LazyFrame::scan_parquet("fixtures/sample.parquet", ScanArgsParquet::default())?
        .select([all(), cols(["finished"]).count().alias("count")])
        .collect()?;

    println!("{}", df);
    Ok(())
}
