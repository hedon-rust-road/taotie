use arrow::array::RecordBatch;
use datafusion::prelude::{CsvReadOptions, SessionContext};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = SessionContext::new();

    // create the dataframe
    let df = ctx
        .read_csv("./fixtures/juventus.csv", CsvReadOptions::new())
        .await?;

    // execute the plan
    let results: Vec<RecordBatch> = df.collect().await?;

    // format the results
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?;
    println!("{}", pretty_results);
    Ok(())
}
