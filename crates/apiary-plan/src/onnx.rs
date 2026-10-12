//! ONNX models, run by ONNX Runtime through the `ort` crate.
//!
//! Built only with the `onnx` feature, and loading the runtime library when the
//! first model is, not at build time (`load-dynamic`): a Node without ONNX Runtime
//! installed still builds and runs, and refuses `onnx` models with a plain error.
//! Point `ORT_DYLIB_PATH` at the runtime's shared library, or put it where the
//! loader looks.
//!
//! The model takes one input of shape `[rows, features]` (`float32`) and gives one
//! output with a value per row, which becomes the `Float64` of `predict`.

use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Float64Array};
use ort::session::Session;
use ort::value::Tensor;

use apiary_core::{ApiaryError, Result};

use crate::models::{Model, ModelBackend};

fn onnx_err(context: &str, e: impl std::fmt::Display) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("ONNX Runtime: {context}: {e}"),
    }
}

/// The `onnx` backend.
pub struct OnnxBackend;

impl ModelBackend for OnnxBackend {
    fn kind(&self) -> &'static str {
        "onnx"
    }

    fn load(&self, bytes: &[u8]) -> Result<Arc<dyn Model>> {
        let session = Session::builder()
            .map_err(|e| onnx_err("cannot start", e))?
            .commit_from_memory(bytes)
            .map_err(|e| onnx_err("cannot load the model", e))?;
        let input = session
            .inputs()
            .first()
            .map(|i| i.name().to_string())
            .ok_or_else(|| ApiaryError::Config {
                message: "The ONNX model has no input".into(),
            })?;
        // The feature count is the second dimension of the input, if it says.
        let arity = session
            .inputs()
            .first()
            .and_then(|i| i.dtype().tensor_shape().and_then(|s| s.get(1).copied()))
            .filter(|d| *d > 0)
            .map(|d| d as usize);
        Ok(Arc::new(Onnx {
            session: Mutex::new(session),
            input,
            arity,
        }))
    }
}

struct Onnx {
    session: Mutex<Session>,
    input: String,
    arity: Option<usize>,
}

impl Model for Onnx {
    fn arity(&self) -> usize {
        // A model that does not fix its width takes whatever it is given.
        self.arity.unwrap_or(0)
    }

    fn accepts(&self, features: usize) -> bool {
        self.arity.is_none_or(|a| a == features)
    }

    fn predict(&self, features: &[ArrayRef]) -> Result<ArrayRef> {
        let rows = features.first().map_or(0, |f| f.len());
        let columns: Vec<&Float64Array> = features
            .iter()
            .map(|f| {
                f.as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| ApiaryError::Internal {
                        message: "A model's features must be Float64".into(),
                    })
            })
            .collect::<Result<_>>()?;
        // Row-major: each row's features together.
        let mut data = Vec::with_capacity(rows * columns.len());
        for row in 0..rows {
            for column in &columns {
                data.push(if column.is_null(row) {
                    f32::NAN
                } else {
                    column.value(row) as f32
                });
            }
        }
        let input = Tensor::from_array(([rows, columns.len()], data))
            .map_err(|e| onnx_err("cannot make the input", e))?;
        let mut session = self.session.lock().expect("onnx session");
        let outputs = session
            .run(ort::inputs![self.input.as_str() => input])
            .map_err(|e| onnx_err("the model failed", e))?;
        let (_, values) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| onnx_err("unreadable output", e))?;
        if values.len() != rows {
            return Err(ApiaryError::Internal {
                message: format!(
                    "The ONNX model gave {} values for {rows} rows",
                    values.len()
                ),
            });
        }
        let out: Float64Array = values
            .iter()
            .map(|v| (!v.is_nan()).then_some(f64::from(*v)))
            .collect();
        Ok(Arc::new(out))
    }
}
