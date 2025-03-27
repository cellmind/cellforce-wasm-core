#[macro_use]
use anyhow;
use wasmtime::Module;
use wasmtime::*;
use wasmtime::{AsContextMut, TypedFunc};
use wasmtime::{Caller, Instance};
use wasmtime_wasi::WasiCtx;
use crate::errors::WasmError;

pub struct WasmState {
    pub wasi: WasiCtx,
}

pub enum WasmModule {
    Data(Vec<u8>),
    Module(Module),
}

/// Wrapper around the allocate function of the WASM module to allocate shared WASM memory. Allocate some memory for the application to write data for the module
/// Note: It is up to the application (and not the WASM module) to provide enough pages, so the module does not run out of memory
/// # Arguments
/// * `size` - size of memory to allocaten
/// returns a pointer to the allocated memory area
pub fn wrapper_wasm_allocate(
    instance: Instance,
    mut store: impl AsContextMut<Data = WasmState>,
    size: u32,
) -> Result<*const u8, WasmError> {
    // Load function an instantiate it

    // get the function
    let func_def = instance
        .get_func(&mut store, "_cellforce_malloc")
        .expect("`wasm_allocate` was not an exported function");
    // validate that it corresponds to the parameters and return types we need
    let func_validated = func_def.typed::<u32, u32>(&store).unwrap();
    // call function
    let result = func_validated.call(store, size).unwrap();
    Ok(result as *const u8)
}

///  Wrapper around the deallocate function of the WASM module to deallocate shared WASM memory. Deallocates existing memory for the purpose of the application
/// # Arguments
/// * `ptr` - mutuable pointer to the memory to deallocate
/// returns a code if it was successful or not
pub fn wrapper_wasm_deallocate(
    instance: Instance,
    mut store: impl AsContextMut<Data = WasmState>,
    ptr: *const u8,
) -> Result<i32, WasmError> {
    // get the function
    let func_def = instance
        .get_func(&mut store, "_cellforce_free")
        .expect("`wasm_deallocate` was not an exported function");
    // validate that it corresponds to the parameters and return types we need
    let func_validated = func_def.typed::<u32, ()>(&store).unwrap();
    // call function
    func_validated.call(store, ptr as u32).unwrap();
    Ok(0)
}
