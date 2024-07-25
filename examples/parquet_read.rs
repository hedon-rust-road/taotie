use std::fs::File;

use arrow_cast::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() -> anyhow::Result<()> {
    let path = "./fixtures/file.parquet";
    let file = File::open(path)?;

    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(10)
        .build()?;

    let mut batches = Vec::new();

    for batch in parquet_reader {
        batches.push(batch?);
    }

    print_batches(&batches[..1])?;
    Ok(())
}
