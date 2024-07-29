use arrow::array::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionContext};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // register csv dataset
    let ctx = SessionContext::new();
    ctx.register_csv("example", "fixtures/juventus.csv", CsvReadOptions::new())
        .await?;

    // create a plan
    let df = ctx.sql("SELECT * FROM example").await?;

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await?;

    // format the result
    let pretty = arrow::util::pretty::pretty_format_batches(&results)?;
    println!("{pretty}");

    Ok(())
}
