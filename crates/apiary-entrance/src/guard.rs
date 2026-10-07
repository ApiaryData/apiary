//! Guards: what an entrance admits.
//!
//! A Guard checks a batch against its Frame's schema before it lands in the
//! crop, as an entrance guard checks a returning bee's colony odour. A match is
//! admitted; anything else is refused, and what happens next depends on who is
//! asking:
//!
//! - a **caller** (a Flight client, a Python call) is waiting, so it gets an
//!   error saying why and keeps its data;
//! - a **stream** (an MQTT topic) has nobody to tell, so the deposit is set
//!   aside with its reason rather than dropped.
//!
//! A batch is admitted when every column it carries is one the Frame has, with
//! a type that can be cast to the Frame's; columns it omits are filled with
//! nulls if the Frame allows. A column the Frame does not have is refused, not
//! dropped: silently discarding a field would lose data. (Evolving the Frame to
//! take new columns is not done yet.)

use std::sync::Arc;

use arrow::compute::can_cast_types;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use apiary_core::{ApiaryError, Result};
use apiary_runtime::{ApiaryNode, IngestResult};

use crate::set_aside::{SetAside, SetAsideRecord};

/// Batches larger than this (in memory) are refused: one deposit should not be
/// able to exhaust a Pi.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;

/// Who is depositing, which decides what refusal means.
#[derive(Clone, Debug)]
pub enum Source {
    /// A caller is waiting for the answer: refuse with an error.
    Caller(String),
    /// A stream with nobody to tell: set the deposit aside.
    Stream(String),
}

impl Source {
    fn name(&self) -> &str {
        match self {
            Source::Caller(name) | Source::Stream(name) => name,
        }
    }
}

/// What the Guard did with a deposit.
#[derive(Clone, Debug)]
pub enum Admission {
    /// It landed in the crop.
    Landed(IngestResult),
    /// It was refused and set aside.
    SetAside(SetAsideRecord),
}

/// Guards a Node's entrance.
#[derive(Clone)]
pub struct Guard {
    node: Arc<ApiaryNode>,
    set_aside: SetAside,
    max_batch_bytes: usize,
}

impl Guard {
    /// A Guard for `node`, setting refused stream deposits aside in `set_aside`.
    pub fn new(node: Arc<ApiaryNode>, set_aside: SetAside) -> Self {
        Self {
            node,
            set_aside,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
        }
    }

    /// Change the largest batch (in memory) the Guard admits.
    pub fn with_max_batch_bytes(mut self, bytes: usize) -> Self {
        self.max_batch_bytes = bytes;
        self
    }

    /// The Node this Guard admits to.
    pub fn node(&self) -> &Arc<ApiaryNode> {
        &self.node
    }

    /// Where refused stream deposits go.
    pub fn set_aside(&self) -> &SetAside {
        &self.set_aside
    }

    /// Admit a batch to a Frame, or refuse it.
    ///
    /// Errors other than a refusal (the Frame does not exist, the disk failed)
    /// are returned as errors whoever is depositing.
    pub async fn admit(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        batch: &RecordBatch,
        source: &Source,
    ) -> Result<Admission> {
        let expected = self.node.frame_schema(hive, box_name, frame).await?;
        if let Err(reason) = check_batch(&expected, batch, self.max_batch_bytes) {
            return self.refuse(hive, box_name, frame, batch, source, reason);
        }
        match self.node.ingest(hive, box_name, frame, batch).await {
            Ok(result) => Ok(Admission::Landed(result)),
            // The values did not fit the types (a number too big, a null in a
            // required column): the same as a schema mismatch.
            Err(ApiaryError::Schema { message }) => {
                self.refuse(hive, box_name, frame, batch, source, message)
            }
            Err(other) => Err(other),
        }
    }

    /// Set aside a payload that never parsed into rows. Used by sources that
    /// decode their own payloads (MQTT).
    pub fn set_aside_raw(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        source: &str,
        reason: &str,
        payload: &[u8],
    ) -> Result<SetAsideRecord> {
        self.set_aside.put_raw(
            &format!("{hive}.{box_name}.{frame}"),
            source,
            reason,
            payload,
        )
    }

    fn refuse(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
        batch: &RecordBatch,
        source: &Source,
        reason: String,
    ) -> Result<Admission> {
        match source {
            Source::Caller(_) => Err(ApiaryError::Schema { message: reason }),
            Source::Stream(_) => {
                let name = format!("{hive}.{box_name}.{frame}");
                tracing::warn!(frame = %name, source = %source.name(), %reason, "Deposit set aside");
                self.set_aside
                    .put_batch(&name, source.name(), &reason, batch)
                    .map(Admission::SetAside)
            }
        }
    }
}

/// Check a batch against a Frame's schema. `Err` says why it is refused.
pub fn check_batch(
    expected: &Schema,
    batch: &RecordBatch,
    max_bytes: usize,
) -> std::result::Result<(), String> {
    let bytes = batch.get_array_memory_size();
    if bytes > max_bytes {
        return Err(format!(
            "The batch is {bytes} bytes, over the {max_bytes} byte limit"
        ));
    }
    check_schema(expected, &batch.schema())
}

/// Check a batch's schema against a Frame's.
pub fn check_schema(expected: &Schema, got: &Schema) -> std::result::Result<(), String> {
    // The common case: the same columns and types.
    if fingerprint(expected) == fingerprint(got) {
        return Ok(());
    }

    let unknown: Vec<&str> = got
        .fields()
        .iter()
        .filter(|f| expected.field_with_name(f.name()).is_err())
        .map(|f| f.name().as_str())
        .collect();
    if !unknown.is_empty() {
        let has: Vec<&str> = expected
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        return Err(format!(
            "The frame has no column named {}; its columns are {}",
            quote_list(&unknown),
            quote_list(&has)
        ));
    }

    let missing: Vec<&str> = expected
        .fields()
        .iter()
        .filter(|f| !f.is_nullable() && got.field_with_name(f.name()).is_err())
        .map(|f| f.name().as_str())
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "The batch lacks required column(s) {}",
            quote_list(&missing)
        ));
    }

    for field in got.fields() {
        let want = expected
            .field_with_name(field.name())
            .expect("unknown columns were refused above");
        if !can_cast_types(field.data_type(), want.data_type()) {
            return Err(format!(
                "Column '{}' is {} but the frame stores {}",
                field.name(),
                field.data_type(),
                want.data_type()
            ));
        }
    }
    Ok(())
}

/// A stable hash of a schema's column names, types and nullability, in order.
/// Two schemas with the same fingerprint are the same for admission.
pub fn fingerprint(schema: &Schema) -> u64 {
    // FNV-1a over a canonical rendering.
    let mut hash: u64 = 0xcbf29ce484222325;
    let mut feed = |bytes: &[u8]| {
        for b in bytes {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    };
    for field in schema.fields() {
        feed(field.name().as_bytes());
        feed(&[0]);
        feed(format!("{}", field.data_type()).as_bytes());
        feed(&[u8::from(field.is_nullable()), 0xff]);
    }
    hash
}

fn quote_list(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn frame() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("temp", DataType::Float64, true),
        ])
    }

    #[test]
    fn identical_schemas_are_admitted() {
        assert!(check_schema(&frame(), &frame()).is_ok());
    }

    #[test]
    fn fingerprint_depends_on_names_types_and_order() {
        let a = frame();
        let b = Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("id", DataType::Int64, false),
        ]);
        let c = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("temp", DataType::Float64, true),
        ]);
        assert_ne!(fingerprint(&a), fingerprint(&b));
        assert_ne!(fingerprint(&a), fingerprint(&c));
        assert_eq!(fingerprint(&a), fingerprint(&frame()));
    }

    #[test]
    fn a_narrower_type_is_admitted_and_a_subset_of_nullable_columns_too() {
        let got = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        assert!(check_schema(&frame(), &got).is_ok());
    }

    #[test]
    fn an_unknown_column_is_refused_with_the_frame_columns() {
        let got = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("humidity", DataType::Float64, true),
        ]);
        let why = check_schema(&frame(), &got).unwrap_err();
        assert!(why.contains("'humidity'") && why.contains("'id'"), "{why}");
    }

    #[test]
    fn a_missing_required_column_is_refused() {
        let got = Schema::new(vec![Field::new("temp", DataType::Float64, true)]);
        let why = check_schema(&frame(), &got).unwrap_err();
        assert!(why.contains("'id'"), "{why}");
    }

    #[test]
    fn an_uncastable_type_is_refused() {
        let got = Schema::new(vec![Field::new(
            "id",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        )]);
        let why = check_schema(&frame(), &got).unwrap_err();
        assert!(why.contains("'id'"), "{why}");
    }

    #[test]
    fn an_oversized_batch_is_refused() {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        assert!(check_batch(&frame(), &batch, 8).is_err());
        assert!(check_batch(&frame(), &batch, 1 << 20).is_ok());
    }
}
