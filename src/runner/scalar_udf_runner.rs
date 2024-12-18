#[macro_use]
use anyhow;
use std::cell::RefCell;
use wasmtime::Module;
use wasmtime::Store;
use wasmtime::*;
use wasmtime::{AsContextMut, TypedFunc};
use wasmtime::{Caller, Instance};
use wasmtime::{Engine, Val};
use wasmtime::{Extern, Func, Linker};
use wasmtime_wasi::{sync::WasiCtxBuilder, WasiCtx};

use arrow::array::{Float32Array, Int64Array, StringBuilder};
use std::ffi::CString;
use std::num::TryFromIntError;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, StringArray, StructArray, TimestampSecondArray,
    UInt64Array,
};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use crate::errors::WasmError;
use crate::runner::binds::wasm_ops::{WasmState, wrapper_wasm_allocate};
use crate::runner::runner_base::WasmUdfRunner;

pub struct WasmArrowScalarUdfRunner {
    engine: Engine,
    module: Module,
    func: String,
    input_types: Vec<DataType>,
    output_type: DataType,
}

impl WasmArrowScalarUdfRunner {
    pub fn new(
        engine: Engine,
        module: Module,
        func: String,
        input_types: Vec<DataType>,
        output_type: DataType,
    ) -> Result<Self, WasmError> {
        Ok(Self {
            engine,
            module,
            func,
            input_types,
            output_type,
        })
    }

    pub fn new_from_raw(
        func: String,
        input_types: Vec<DataType>,
        output_type: DataType,
        wasm_data: &[u8],
    ) -> Result<Self, WasmError> {
        let engine = Engine::default();
        let module = Module::from_binary(&engine, wasm_data).unwrap();
        Ok(Self {
            engine,
            module,
            func,
            input_types,
            output_type,
        })
    }
}

impl WasmUdfRunner for WasmArrowScalarUdfRunner {
    fn run(&self, batch: &RecordBatch) -> Result<RecordBatch, WasmError> {
        let mut linker = Linker::<WasmState>::new(&self.engine);
        wasmtime_wasi::add_to_linker(&mut linker, |state: &mut WasmState| &mut state.wasi).unwrap();
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args().unwrap()
            .build();
        let mut store = Store::new(&self.engine, WasmState { wasi: wasi });


        linker.module(&mut store, "", &self.module).unwrap();
        let instance: Instance = linker
            .instantiate(&mut store, &self.module).unwrap();

        let func_def = instance
            .get_func(&mut store, &self.func)
            .expect(format!("`{}` was not an exported function", &self.func).as_str());

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(WasmError::GeneralError { msg: format!(
                "failed to find `memory` export"
            )}).unwrap();

        let schema = batch.schema();
        let mut input_vals = vec![];
        for i in 0..batch.num_columns() {
            let field = schema.field(i);
            let array = batch.column(i);
            let col_batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![field.clone()])),
                vec![array.clone()],
            )
            .unwrap();

            let buffer: Vec<u8> = Vec::new();
            let mut stream_writer = StreamWriter::try_new(buffer, &col_batch.schema()).unwrap();
            stream_writer
                .write(&col_batch).unwrap();
            stream_writer.finish().unwrap();
            let serialized_data = stream_writer
                .into_inner().unwrap();
            let serialized_data_size = serialized_data.len();

            let offset_data: u32 =
                wrapper_wasm_allocate(instance, &mut store, serialized_data_size as u32).unwrap() as u32;
            memory
                .write(
                    &mut store,
                    offset_data.try_into().unwrap(),
                    serialized_data.as_slice(),
                ).unwrap();

            let ptr = ((serialized_data_size as u64) << 32) + offset_data as u64;
            input_vals.push(Val::I64(ptr as i64));
        }

        let mut tmp_result_vals = vec![Val::I64(0)];
        let mut_store = &mut store;
        func_def
            .call(mut_store, input_vals.as_slice(), &mut tmp_result_vals).unwrap();
        let result_val = match tmp_result_vals.get(0) {
            Some(v) => v.clone(),
            None => return Err(WasmError::GeneralError {
                msg: "hello".to_string()
            }),
            _ => return Err(WasmError::GeneralError {
                msg: "hello".to_string()
            }),
        };

        if let Val::I64(result_ptr) = result_val {
            let result_size = (result_ptr >> 32) as u32;
            let result_offset = (result_ptr & 0xffffffff) as u32;

            if result_size == 0 {
                return Err(WasmError::GeneralError { msg: format!(
                    "Error: No valid answer received from function"
                )});
            } else {
                let memory = instance
                    .get_memory(&mut store, "memory")
                    .ok_or(anyhow::format_err!("failed to find `memory` export"))
                    .unwrap();

                let mut result_arrow_ipc: Vec<u8> = vec![0; result_size as usize];
                let mut result_arrow_ipc_buffer = result_arrow_ipc.as_mut_slice();
                memory
                    .read(&store, result_offset as usize, &mut result_arrow_ipc_buffer).unwrap();

                let stream_reader = StreamReader::try_new(result_arrow_ipc.as_slice(), None).unwrap();

                let mut batches = vec![];
                for item in stream_reader {
                    batches.push(item.unwrap());
                }

                let result_batch = concat_batches(&batches[0].schema(), &batches).unwrap();

                return Ok(result_batch);
            }
        } else {
            return Err(WasmError::GeneralError { msg: format!(
                "Error: No valid answer received from function"
            )});
        }
    }
}

pub struct WasmScalarUdfRunner {
    engine: Engine,
    module: Module,
    func: String,
    input_types: Vec<DataType>,
    output_type: DataType,
}

impl WasmScalarUdfRunner {
    pub fn new(
        engine: Engine,
        module: Module,
        func: String,
        input_types: Vec<DataType>,
        output_type: DataType,
    ) -> Result<Self, WasmError> {
        Ok(Self {
            engine,
            module,
            func,
            input_types,
            output_type,
        })
    }

    pub fn new_from_raw(
        func: String,
        input_types: Vec<DataType>,
        output_type: DataType,
        wasm_data: &[u8],
    ) -> Result<Self, WasmError> {
        let engine = Engine::default();
        let module = Module::from_binary(&engine, wasm_data).unwrap();
        Ok(Self {
            engine,
            module,
            func,
            input_types,
            output_type,
        })
    }
}

impl WasmUdfRunner for WasmScalarUdfRunner {
    fn run(&self, batch: &RecordBatch) -> Result<RecordBatch, WasmError> {
        let mut linker = Linker::<WasmState>::new(&self.engine);
        wasmtime_wasi::add_to_linker(&mut linker, |state: &mut WasmState| &mut state.wasi).unwrap();
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .inherit_args().unwrap()
            .build();
        let mut store = Store::new(&self.engine, WasmState { wasi: wasi });

        linker.module(&mut store, "", &self.module).unwrap();
        let instance: Instance = linker
            .instantiate(&mut store, &self.module).unwrap();
        let func_def = instance
            .get_func(&mut store, &self.func)
            .expect(format!("`{}` was not an exported function", &self.func).as_str());

        let mut input_arrow_types = vec![];
        let schema = batch.schema();
        for field in schema.fields() {
            input_arrow_types.push(field.data_type());
        }

        let mut result_vals = vec![];
        for row_indice in 0..batch.num_rows() {
            let mut input_vals = vec![];
            for (col_indice, arrow_type) in input_arrow_types.clone().into_iter().enumerate() {
                let array = batch.column(col_indice);
                let val = match arrow_type {
                    DataType::Utf8 => {
                        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                        let val = array.value(row_indice);
                        let param_name_cstring: CString = CString::new(val).unwrap();
                        let param_name_cstring_as_bytes: &[u8] =
                            param_name_cstring.to_bytes_with_nul();
                        let size = param_name_cstring_as_bytes.len() as u32;
                        let offset: u32 =
                            wrapper_wasm_allocate(instance, &mut store, size).unwrap() as u32;
                        let memory = instance
                            .get_memory(&mut store, "memory")
                            .ok_or(anyhow::format_err!("failed to find `memory` export"))
                            .unwrap();
                        memory
                            .write(
                                &mut store,
                                offset.try_into().unwrap(),
                                param_name_cstring_as_bytes,
                            )
                            .unwrap();
                        let ptr = ((size as u64) << 32) + offset as u64;
                        Val::I64(ptr as i64)
                    }
                    DataType::Int32 => {
                        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                        let val = array.value(row_indice);
                        Val::I32(val)
                    }
                    DataType::Int64 => {
                        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                        let val = array.value(row_indice);
                        Val::I64(val)
                    }
                    DataType::Float32 => {
                        let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                        let val = array.value(row_indice);
                        Val::F32(val.to_bits())
                    }
                    DataType::Float64 => {
                        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        let val = array.value(row_indice);
                        Val::F64(val.to_bits())
                    }
                    _ => unimplemented!(),
                };
                input_vals.push(val)
            }
            let mut tmp_result_vals = vec![Val::I64(0)];
            let mut_store = &mut store;
            func_def
                .call(mut_store, input_vals.as_slice(), &mut tmp_result_vals).unwrap();
            let result_ptr = match tmp_result_vals.get(0) {
                Some(v) => v.clone(),
                None => return Err("hello".into()),
                _ => return Err("hello".into()),
            };
            result_vals.push(result_ptr);
        }

        let batch = match self.output_type {
            DataType::Utf8 => {
                let mut result_vecs = vec![];
                for result_val in result_vals {
                    if let Val::I64(result_ptr) = result_val {
                        let result_size = (result_ptr >> 32) as u32;
                        let result_offset = (result_ptr & 0xffffffff) as u32;

                        if result_size == 0 {
                            return Err(format!(
                                "Error: No valid answer received from function"
                            ).into());
                        } else {
                            let mut result_offset_position = result_offset.clone();

                            let memory = instance
                                .get_memory(&mut store, "memory")
                                .ok_or(anyhow::format_err!("failed to find `memory` export"))
                                .unwrap();

                            let mut result_arrow_ipc: Vec<u8> = vec![0; result_size as usize];
                            let mut result_arrow_ipc_buffer = result_arrow_ipc.as_mut_slice();

                            memory
                                .read(
                                    &store,
                                    result_offset_position.try_into().unwrap(),
                                    &mut result_arrow_ipc_buffer,
                                )
                                .unwrap();
                            result_vecs.push(result_arrow_ipc);
                        }
                    } else {
                        return Err(format!(
                            "Error: No valid answer received from function"
                        ).into());
                    }
                }
                let mut result_values = vec![];
                for result_vec in result_vecs {
                    let result_str = String::from_utf8(result_vec.to_vec()).unwrap();
                    result_values.push(result_str);
                }
                let schema = Schema::new(vec![Field::new("", DataType::Utf8, true)]);
                let result_values = StringArray::from(result_values);
                let result_batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(result_values)])
                        .unwrap();
                result_batch
            }
            DataType::Int32 => {
                let mut result_values = vec![];
                for result_val in result_vals {
                    if let Val::I32(value) = result_val {
                        result_values.push(value);
                    } else {
                        return Err(format!(
                            "Error: No valid answer received from function"
                        ).into());
                    }
                }
                let schema = Schema::new(vec![Field::new("", DataType::Int32, true)]);
                let result_values = Int32Array::from(result_values);
                let result_batch =
                    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(result_values)])
                        .unwrap();
                result_batch
            }
            DataType::Int64 => {
                let mut result_values = vec![];
                for result_val in result_vals {
                    if let Val::I64(value) = result_val {
                        result_values.push(value);
                    } else {
                        return Err(format!(
                            "Error: No valid answer received from function"
                        ).into());
                    }
                }
                let schema = Schema::new(vec![Field::new("", DataType::Int64, true)]);
                let result_values = Int64Array::from(result_values);
                let result_batch =
                    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(result_values)])
                        .unwrap();
                result_batch
            }
            DataType::Float32 => {
                let mut result_values = vec![];
                for result_val in result_vals {
                    if let Val::F32(value) = result_val {
                        result_values.push(f32::from_bits(value));
                    } else {
                        return Err(format!(
                            "Error: No valid answer received from function"
                        ).into());
                    }
                }
                let schema = Schema::new(vec![Field::new("", DataType::Float32, true)]);
                let result_values = Float32Array::from(result_values);
                let result_batch =
                    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(result_values)])
                        .unwrap();
                result_batch
            }
            DataType::Float64 => {
                let mut result_values = vec![];
                for result_val in result_vals {
                    if let Val::F64(value) = result_val {
                        result_values.push(f64::from_bits(value));
                    } else {
                        return Err(format!(
                            "Error: No valid answer received from function"
                        ).into());
                    }
                }
                let schema = Schema::new(vec![Field::new("", DataType::Float64, true)]);
                let result_values = Float64Array::from(result_values);
                let result_batch =
                    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(result_values)])
                        .unwrap();
                result_batch
            }
            _ => {
                unimplemented!()
            }
        };
        Ok(batch)
    }
}
