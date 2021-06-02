use std::sync::Arc;

use arrow::{
    array::{as_primitive_array, Array, UInt32Array},
    datatypes::{DataType, Field, Schema},
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};

fn main() {
    let array = UInt32Array::from(vec![Some(1), Some(2), Some(3)]);
    assert_eq!(vec![Some(1), Some(2), Some(3)], array.iter().collect::<Vec<_>>());

    let sliced = array.slice(1, 2);
    let read_sliced: &UInt32Array = as_primitive_array(&sliced);
    assert_eq!(vec![Some(2), Some(3)], read_sliced.iter().collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, true)])),
        vec![Arc::new(array)],
    ).expect("new batch");

    let mut writer = StreamWriter::try_new(vec![], &batch.schema()).expect("new writer");
    writer.write(&batch).expect("write");
    let outbuf = writer.into_inner().expect("inner");

    let mut reader = StreamReader::try_new(&outbuf[..]).expect("new reader");
    let read_batch = reader.next().unwrap().expect("read batch");

    let read_array: &UInt32Array = as_primitive_array(read_batch.column(0));
    assert_eq!(vec![Some(2), Some(3)], read_array.iter().collect::<Vec<_>>());
}
