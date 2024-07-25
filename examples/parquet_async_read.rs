use std::time::SystemTime;

use arrow::{
    array::{Scalar, StringArray},
    compute::kernels::cmp::eq,
};
use arrow_cast::pretty::print_batches;
use futures::TryStreamExt;
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, RowFilter},
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use tokio::fs::File;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let path = "./fixtures/file.parquet";
    let file = File::open(path).await.unwrap();

    // Create a async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let mut builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .unwrap()
        .with_batch_size(10);

    let file_metadata = builder.metadata().file_metadata().clone();
    let mask = ProjectionMask::roots(file_metadata.schema_descr(), vec![0, 1, 2]);

    // Set projection mask to read only root columns 1 and 2.
    builder = builder.with_projection(mask);

    // Highlight: set `RowFilter`, it'll push down filter predicates to skip IO and decode.
    // For more specific usage: please refer to https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/physical_plan/file_format/parquet/row_filter.rs.
    let scalar = StringArray::from(vec![""]);
    let filter = ArrowPredicateFn::new(
        ProjectionMask::roots(file_metadata.schema_descr(), [0]),
        move |record_batch| eq(record_batch.column(0), &Scalar::new(&scalar)),
    );
    let row_filter = RowFilter::new(vec![Box::new(filter)]);
    builder = builder.with_row_filter(row_filter);

    // Build a async parquet reader.
    let stream = builder.build().unwrap();

    let start = SystemTime::now();

    let result = stream.try_collect::<Vec<_>>().await.unwrap();

    println!("took: {} ms", start.elapsed()?.as_millis());

    print_batches(&result)?;

    Ok(())
}
