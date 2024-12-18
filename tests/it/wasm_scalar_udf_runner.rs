use arrow::array::{Int32Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::log::Record;
use cellgen_query::wasm::runner::runner_base::WasmUdfRunner;
use cellgen_query::wasm::runner::scalar_udf_runner::{WasmArrowScalarUdfRunner, WasmScalarUdfRunner};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_wasm_scalar_udf_runner() {
    let expected_add_result_batch = create_add_expect_data();
    let expected_concat_result_batch = create_concat_expect_data();

    let root_path = format!(
        "{}/../../data",
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).to_str().unwrap()
    );
    let path = format!("{}/wasm/cellgen_wasm_udf_examples.wasm", root_path);
    let wasm_data = std::fs::read(path).unwrap();
    let runner = WasmScalarUdfRunner::new_from_raw(
        "concat".to_string(),
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        &wasm_data,
    )
    .unwrap();
    let batch = create_str_input_data();
    let result_batch = runner.run(&batch).unwrap();
    assert_eq!(result_batch, expected_concat_result_batch);

    let runner = WasmScalarUdfRunner::new_from_raw(
        "add".to_string(),
        vec![DataType::Int32, DataType::Int32],
        DataType::Int32,
        &wasm_data,
    )
    .unwrap();
    let batch = create_int_input_data();
    let result_batch = runner.run(&batch).unwrap();
    assert_eq!(result_batch, expected_add_result_batch);
}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_wasm_arrow_scalar_udf_runner() {
    let expected_add_result_batch = create_add_expect_data();
    let expected_concat_result_batch = create_concat_expect_data();

    let root_path = format!(
        "{}/../../data",
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).to_str().unwrap()
    );

    let path = format!("{}/wasm/cellgen_wasm_udf_examples.wasm", root_path);
    let wasm_data = std::fs::read(path).unwrap();
    let runner = WasmArrowScalarUdfRunner::new_from_raw(
        "concat_arrow".to_string(),
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        &wasm_data,
    )
    .unwrap();
    let batch = create_str_input_data();
    let result_batch = runner.run(&batch).unwrap();
    assert_eq!(result_batch, expected_concat_result_batch);

    let runner = WasmArrowScalarUdfRunner::new_from_raw(
        "add_arrow".to_string(),
        vec![DataType::Int32, DataType::Int32],
        DataType::Int32,
        &wasm_data,
    )
    .unwrap();
    let batch = create_int_input_data();
    let result_batch = runner.run(&batch).unwrap();
    assert_eq!(result_batch, expected_add_result_batch);
}

fn create_int_input_data() -> RecordBatch {
    // define schema
    let schema = Schema::new(vec![
        Field::new("val1", DataType::Int32, true),
        Field::new("val2", DataType::Int32, true),
    ]);
    let val1 = Int32Array::from(vec![6]);
    let val2 = Int32Array::from(vec![8]);

    // build a record batch
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(val1), Arc::new(val2)],
    )
    .unwrap();
    batch
}

fn create_one_col_str_input_data() -> RecordBatch {
    // define schema
    let value = r#"{"column":{"contentType":"TEXT","contentSource":{"directory":{"uri":{"source":{"localFile":{"rootPath":"./"}},"options":{"localFile":{"path":"../data"}}}}}},"cell":{"contentSource":{"relativeFile":{"relativePath":"iris-csv/iris.csv"}}}}"#;
    let schema = Schema::new(vec![Field::new("val1", DataType::Utf8, true)]);
    let contents = StringArray::from(vec![value.to_string()]);

    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(contents)])
            .unwrap();
    batch
}

fn create_str_input_data() -> RecordBatch {
    // define schema
    let schema = Schema::new(vec![
        Field::new("val1", DataType::Utf8, true),
        Field::new("val2", DataType::Utf8, true),
    ]);
    let contents = StringArray::from(vec![String::from_utf8_lossy("hello".as_bytes()).to_string()]);
    let titles = StringArray::from(vec![String::from_utf8_lossy("world".as_bytes()).to_string()]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(contents), Arc::new(titles)],
    )
    .unwrap();
    batch
}

fn create_add_expect_data() -> RecordBatch {
    let schema = Schema::new(vec![Field::new("", DataType::Int32, true)]);
    let contents = Int32Array::from(vec![14]);
    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(contents)])
            .unwrap();
    batch
}

fn create_concat_expect_data() -> RecordBatch {
    let schema = Schema::new(vec![Field::new("", DataType::Utf8, true)]);
    let contents = StringArray::from(vec![String::from_utf8_lossy("helloworld".as_bytes()).to_string()]);
    let batch =
        RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(contents)])
            .unwrap();
    batch
}
