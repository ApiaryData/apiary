//! Model functions: `predict('model', feature, ...)` in SQL.
//!
//! Features are SQL; a model is a scalar function over them. Models are files in
//! the comb store, under `models/<name>/`, so every Node of a colony reads the
//! same ones: a Node that runs a Patch calling `predict` fetches the model the
//! first time it needs it, and again when a newer version is registered.
//!
//! A model is run by a [`ModelBackend`] that knows its kind. The `linear` backend
//! (a weight per feature and a bias, as JSON) is built in and needs nothing else;
//! the `onnx` backend, behind the `onnx` feature, runs any ONNX model through
//! ONNX Runtime.
//!
//! The function itself is synchronous, as DataFusion's are, so the models a query
//! uses are fetched before it runs: [`Models::preload_for`] reads the plan for the
//! models its `predict` calls name.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use bytes::Bytes;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};

use apiary_core::{ApiaryError, Result, StorageBackend};

/// The name of the SQL function.
pub const PREDICT: &str = "predict";

/// A loaded model.
pub trait Model: Send + Sync {
    /// How many features it takes.
    fn arity(&self) -> usize;

    /// Whether it takes `features` features. A model that does not fix its width
    /// takes any number.
    fn accepts(&self, features: usize) -> bool {
        features == self.arity()
    }

    /// Predict for each row of `features` (one array per feature, all the same
    /// length, as `Float64`).
    fn predict(&self, features: &[ArrayRef]) -> Result<ArrayRef>;
}

/// Loads models of one kind.
pub trait ModelBackend: Send + Sync {
    /// The kind of model it loads, as written in a model's manifest.
    fn kind(&self) -> &'static str;

    /// Load a model from its file.
    fn load(&self, bytes: &[u8]) -> Result<Arc<dyn Model>>;
}

/// A weight for each feature and a bias: `bias + Σ weight · feature`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Linear {
    /// One weight per feature.
    pub weights: Vec<f64>,
    /// Added to the weighted sum.
    pub bias: f64,
}

impl Model for Linear {
    fn arity(&self) -> usize {
        self.weights.len()
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
        let out: Float64Array = (0..rows)
            .map(|row| {
                // A missing feature makes the prediction missing.
                columns
                    .iter()
                    .zip(&self.weights)
                    .try_fold(self.bias, |sum, (column, weight)| {
                        (!column.is_null(row)).then(|| sum + weight * column.value(row))
                    })
            })
            .collect();
        Ok(Arc::new(out))
    }
}

/// The built-in backend for [`Linear`] models.
pub struct LinearBackend;

impl ModelBackend for LinearBackend {
    fn kind(&self) -> &'static str {
        "linear"
    }

    fn load(&self, bytes: &[u8]) -> Result<Arc<dyn Model>> {
        let model: Linear = serde_json::from_slice(bytes).map_err(|e| ApiaryError::Config {
            message: format!("Not a linear model: {e}"),
        })?;
        Ok(Arc::new(model))
    }
}

/// What a model's directory in the comb store says about it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Manifest {
    kind: String,
    /// Higher when the model has been registered again.
    version: u64,
}

struct Loaded {
    version: u64,
    model: Arc<dyn Model>,
}

/// How models have been read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ModelCounts {
    /// Model files fetched from the comb store and loaded.
    pub fetched: u64,
    /// Rows predicted.
    pub rows: u64,
}

/// The models a Node can run: kept in the comb store, loaded on demand.
pub struct Models {
    store: Arc<dyn StorageBackend>,
    backends: Vec<Arc<dyn ModelBackend>>,
    loaded: Mutex<HashMap<String, Loaded>>,
    counts: Mutex<ModelCounts>,
}

impl std::fmt::Debug for Models {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Models").finish_non_exhaustive()
    }
}

fn manifest_key(name: &str) -> String {
    format!("models/{name}/manifest.json")
}

fn file_key(name: &str, version: u64) -> String {
    format!("models/{name}/v{version}.model")
}

impl Models {
    /// Models kept in `store`, with the built-in backends.
    pub fn new(store: Arc<dyn StorageBackend>) -> Arc<Self> {
        #[allow(unused_mut)]
        let mut backends: Vec<Arc<dyn ModelBackend>> = vec![Arc::new(LinearBackend)];
        #[cfg(feature = "onnx")]
        backends.push(Arc::new(crate::onnx::OnnxBackend));
        Self::with_backends(store, backends)
    }

    /// Models kept in `store`, run by `backends`.
    pub fn with_backends(
        store: Arc<dyn StorageBackend>,
        backends: Vec<Arc<dyn ModelBackend>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            backends,
            loaded: Mutex::default(),
            counts: Mutex::default(),
        })
    }

    /// Register a model: its file goes into the comb store, and every Node reads
    /// it from there the next time a query names it. Registering a name again
    /// replaces the model. Returns the version it took.
    pub async fn register(&self, name: &str, kind: &str, file: Bytes) -> Result<u64> {
        let backend = self.backend(kind)?;
        // Checked now, so a file that will not load is refused here and not in a
        // query on some other Node.
        backend.load(&file)?;
        let version = match self.manifest(name).await? {
            Some(m) => m.version + 1,
            None => 1,
        };
        self.store.put(&file_key(name, version), file).await?;
        // The manifest last: until it moves, readers keep the version they have.
        let manifest = Manifest {
            kind: kind.to_string(),
            version,
        };
        self.store
            .put(
                &manifest_key(name),
                Bytes::from(serde_json::to_vec(&manifest).expect("a manifest serialises")),
            )
            .await?;
        Ok(version)
    }

    fn backend(&self, kind: &str) -> Result<&Arc<dyn ModelBackend>> {
        self.backends
            .iter()
            .find(|b| b.kind() == kind)
            .ok_or_else(|| ApiaryError::Config {
                message: format!("This Node cannot run '{kind}' models"),
            })
    }

    async fn manifest(&self, name: &str) -> Result<Option<Manifest>> {
        match self.store.get(&manifest_key(name)).await {
            Ok(bytes) => {
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|e| ApiaryError::Internal {
                        message: format!("Unreadable manifest of model '{name}': {e}"),
                    })
            }
            Err(ApiaryError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Have the model called `name` ready: fetched from the comb store if this
    /// Node does not hold its current version.
    pub async fn ensure(&self, name: &str) -> Result<()> {
        let manifest = self
            .manifest(name)
            .await?
            .ok_or_else(|| ApiaryError::NotFound {
                key: format!("model '{name}'"),
            })?;
        let current = self
            .loaded
            .lock()
            .expect("models")
            .get(name)
            .is_some_and(|l| l.version == manifest.version);
        if current {
            return Ok(());
        }
        let file = self.store.get(&file_key(name, manifest.version)).await?;
        let model = self.backend(&manifest.kind)?.load(&file)?;
        self.loaded.lock().expect("models").insert(
            name.to_string(),
            Loaded {
                version: manifest.version,
                model,
            },
        );
        self.counts.lock().expect("model counts").fetched += 1;
        Ok(())
    }

    /// Have every model the `predict` calls in `plan` name ready.
    pub async fn preload_for(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        let mut names: Vec<String> = Vec::new();
        collect_models(plan, &mut names);
        names.sort();
        names.dedup();
        for name in names {
            self.ensure(&name).await?;
        }
        Ok(())
    }

    fn model(&self, name: &str) -> DfResult<Arc<dyn Model>> {
        self.loaded
            .lock()
            .expect("models")
            .get(name)
            .map(|l| Arc::clone(&l.model))
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "model '{name}' has not been loaded on this Node"
                ))
            })
    }

    /// How models have been read.
    pub fn counts(&self) -> ModelCounts {
        *self.counts.lock().expect("model counts")
    }
}

/// The names of the models the `predict` calls in a plan use.
fn collect_models(plan: &Arc<dyn ExecutionPlan>, names: &mut Vec<String>) {
    let _ = plan.apply_expressions(&mut |expr| {
        let _ = expr.apply(|e| {
            if let Some(call) = e.downcast_ref::<ScalarFunctionExpr>()
                && call.name() == PREDICT
                && let Some(first) = call.args().first()
                && let Some(literal) = first.downcast_ref::<Literal>()
                && let ScalarValue::Utf8(Some(name)) | ScalarValue::LargeUtf8(Some(name)) =
                    literal.value()
            {
                names.push(name.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    for child in plan.children() {
        collect_models(child, names);
    }
}

/// `predict('model', feature, ...)`: the model's prediction for each row, as
/// `Float64`. The model's name must be a literal, and its features numbers.
pub fn predict_udf(models: Arc<Models>) -> ScalarUDF {
    ScalarUDF::new_from_impl(Predict {
        models,
        // Volatile, so the planner never folds a call at planning time, before the
        // model the query names has been fetched (a call with only literals would
        // otherwise run on whatever version this Node last held).
        signature: Signature::variadic_any(Volatility::Volatile),
    })
}

struct Predict {
    models: Arc<Models>,
    signature: Signature,
}

impl std::fmt::Debug for Predict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Predict").finish_non_exhaustive()
    }
}

impl PartialEq for Predict {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Predict {}

impl Hash for Predict {
    fn hash<H: Hasher>(&self, state: &mut H) {
        PREDICT.hash(state);
    }
}

impl ScalarUDFImpl for Predict {
    fn name(&self) -> &str {
        PREDICT
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        if arg_types.len() < 2 {
            return Err(DataFusionError::Plan(
                "predict(model, feature, ...) takes a model name and at least one feature".into(),
            ));
        }
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let rows = args.number_rows;
        let mut values = args.args.into_iter();
        let name = match values.next() {
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))))
            | Some(ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(name))))
            | Some(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(name)))) => name,
            _ => {
                return Err(DataFusionError::Plan(
                    "predict's first argument must be a model name written as a literal".into(),
                ));
            }
        };
        let model = self.models.model(&name)?;
        let features: Vec<ArrayRef> = values
            .map(|v| {
                let array = v.into_array(rows)?;
                Ok(cast(&array, &DataType::Float64)?)
            })
            .collect::<DfResult<_>>()?;
        if !model.accepts(features.len()) {
            return Err(DataFusionError::Execution(format!(
                "model '{name}' takes {} features, not {}",
                model.arity(),
                features.len()
            )));
        }
        let out = model
            .predict(&features)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.models.counts.lock().expect("model counts").rows += rows as u64;
        Ok(ColumnarValue::Array(out))
    }
}
