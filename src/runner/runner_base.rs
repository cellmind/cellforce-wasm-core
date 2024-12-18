use arrow_array::RecordBatch;
use crate::errors::WasmError;

pub trait WasmUdfRunner {
    fn run(&self, batch: &RecordBatch) -> Result<RecordBatch, WasmError>;
}