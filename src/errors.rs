
#[derive(thiserror::Error, Debug)]
pub enum WasmError {
    #[error("general wasm error: {msg}")]
    GeneralError { msg: String },
}

impl From<String> for WasmError {
    fn from(msg: String) -> Self {
        WasmError::GeneralError { msg }
    }
}

impl From<&str> for WasmError {
    fn from(msg: &str) -> Self {
        WasmError::GeneralError { msg: msg.into() }
    }
}

