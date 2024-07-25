#![allow(dead_code)]
use std::{iter::Sum, str::FromStr, sync::Arc};

use arrow::{
    array::{
        Array, ArrayAccessor, ArrayIter, ArrayRef, AsArray, Float32Array, Int32Array,
        PrimitiveArray, StringArray, TimestampNanosecondArray, UInt32Array,
    },
    datatypes::{ArrowPrimitiveType, DataType, Field, Int32Type, Schema, UInt32Type},
    error::Result,
    json::ReaderBuilder,
};
use serde::Serialize;

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}

fn main() -> anyhow::Result<()> {
    let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    assert_eq!(array.len(), 3);
    assert_eq!(array.value(0), 1);
    assert!(array.is_null(1));

    let collected: Vec<_> = array.iter().collect();
    assert_eq!(collected, vec![Some(1), None, Some(3)]);
    assert_eq!(array.values(), &[1, 0, 3]);

    // write generic code to calculate sum of array.
    assert_eq!(sum(&Float32Array::from(vec![1.1, 2.9, 3.])), 7.);
    assert_eq!(sum(&TimestampNanosecondArray::from(vec![1, 2, 3])), 6);

    // write generic code to calculate min of array.
    assert_eq!(min(&Int32Array::from(vec![1, 2, 3])), Some(1));
    let empty_vec: Vec<i32> = vec![];
    assert_eq!(min(&Int32Array::from(empty_vec)), None);
    assert_eq!(min(&StringArray::from(vec!["b", "a", "c"])), Some("a"));

    let array = parse_strings(["1", "2", "3"], DataType::Int32);
    let integers = array.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(integers.values(), &[1, 2, 3]);

    let array = parse_strings2(["1", "2", "3"], &DataType::UInt32).unwrap();
    let integers = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(integers.values(), &[1, 2, 3]);

    // with serde
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("string", DataType::Utf8, false),
    ]);

    let rows = vec![
        MyStruct {
            int32: 5,
            string: "bar".to_string(),
        },
        MyStruct {
            int32: 10,
            string: "foo".to_string(),
        },
    ];

    let mut decoder = ReaderBuilder::new(Arc::new(schema))
        .build_decoder()
        .unwrap();
    decoder.serialize(&rows).unwrap();

    let batch = decoder.flush().unwrap().unwrap();

    let int32 = batch.column(0).as_primitive::<Int32Type>();
    assert_eq!(int32.values(), &[5, 10]);
    let string = batch.column(1).as_string::<i32>(); // i32 specifies the offset type
    assert_eq!(string.value(0), "bar");
    assert_eq!(string.value(1), "foo");
    Ok(())
}

fn sum<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>) -> T::Native
where
    T::Native: Sum,
{
    array.iter().map(|v| v.unwrap_or_default()).sum()
}

fn min<T: ArrayAccessor>(array: T) -> Option<T::Item>
where
    T::Item: Ord,
{
    ArrayIter::new(array).flatten().min()
}

fn impl_string(_array: &StringArray) {}
fn impl_f32(_array: &Float32Array) {}

fn impl_dyn1(array: &dyn Array) {
    match array.data_type() {
        arrow::datatypes::DataType::Float32 => impl_f32(array.as_any().downcast_ref().unwrap()),
        arrow::datatypes::DataType::Utf8 => impl_string(array.as_any().downcast_ref().unwrap()),
        _ => unimplemented!(),
    }
}

fn impl_dyn2(array: &dyn Array) {
    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => impl_string(array.as_string()),
        arrow::datatypes::DataType::Float32 => impl_f32(array.as_primitive()),
        _ => unimplemented!(),
    }
}

fn parse_to_primitive<'a, T, I>(iter: I) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: FromStr,
    I: IntoIterator<Item = &'a str>,
{
    PrimitiveArray::from_iter(iter.into_iter().map(|val| T::Native::from_str(val).ok()))
}

fn parse_strings<'a, I>(iter: I, to_data_type: DataType) -> ArrayRef
where
    I: IntoIterator<Item = &'a str>,
{
    match to_data_type {
        DataType::Int32 => Arc::new(parse_to_primitive::<Int32Type, _>(iter)) as _,
        DataType::UInt32 => Arc::new(parse_to_primitive::<UInt32Type, _>(iter)) as _,
        _ => unimplemented!(),
    }
}

fn parse_strings2<'a, I>(iter: I, to_data_type: &DataType) -> Result<ArrayRef>
where
    I: IntoIterator<Item = &'a str>,
{
    let array = StringArray::from_iter(iter.into_iter().map(Some));
    arrow::compute::cast(&array, to_data_type)
}
