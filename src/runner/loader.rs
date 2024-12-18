use std::sync::Arc;

use crate::errors::WasmError;
use crate::runner::datatypes::udf_type_to_arrow_type;
use crate::runner::runner_base::WasmUdfRunner;
use crate::runner::scalar_udf_runner::{WasmArrowScalarUdfRunner, WasmScalarUdfRunner};

#[derive(Clone, PartialEq)]
pub struct WasmScalarUdfOptions {
    pub export_name: String,
    pub internal_name: String,
    pub input_types: Vec<String>,
    pub output_types: Vec<String>,
    pub arrow: bool,
}

pub struct WasmUdfRunnerLoader {}

impl WasmUdfRunnerLoader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn load_scalar_udf_runner(
        spec: &WasmScalarUdfOptions,
        wasm_data: &[u8],
    ) -> Result<Arc<dyn WasmUdfRunner + Sync + Send>, WasmError> {
        let input_types = spec
            .input_types
            .iter()
            .map(|t| udf_type_to_arrow_type(t))
            .collect();
        let output_type = udf_type_to_arrow_type(&spec.output_types[0]);
        match spec.arrow {
            true => Ok(Arc::new(
                WasmArrowScalarUdfRunner::new_from_raw(
                    spec.internal_name.clone(),
                    input_types,
                    output_type,
                    &wasm_data,
                )
                .unwrap(),
            )),
            false => Ok(Arc::new(
                WasmScalarUdfRunner::new_from_raw(
                    spec.internal_name.clone(),
                    input_types,
                    output_type,
                    &wasm_data,
                )
                .unwrap(),
            )),
        }
    }
}
