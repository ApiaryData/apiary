//! Cells as types, and the ripening recipe.
//!
//! A Cell is one data file of a Frame's Delta table. Its ripeness is a type, so
//! the only way to a capping commit is through the ripeness checks:
//!
//! - [`Cell<Nectar>`]: committed, not yet ripened (what a deposit writes)
//! - [`Cell<Ripe>`]: built by ripening nectar, not yet committed
//! - [`Cell<Capped>`]: ripe and checked, ready for the capping commit
//!
//! A [`Recipe`] says how a Frame ripens: the sort key and the deduplication key.
//! It is stored in the Frame's Delta table properties, so any engine reading
//! the table can see it and it travels with the table.

use std::collections::HashMap;
use std::marker::PhantomData;

use arrow::array::UInt32Array;
use arrow::compute::{SortColumn, lexsort_to_indices, take_record_batch};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use deltalake::kernel::Add;

use apiary_core::{ApiaryError, Result};

use crate::comb::CellState;
use crate::comb::STATE_TAG;

/// Table property holding the sort key, as comma-separated column names.
pub const SORT_BY_PROPERTY: &str = "apiary.ripen.sort_by";

/// Table property holding the deduplication key, as comma-separated column names.
pub const DEDUP_BY_PROPERTY: &str = "apiary.ripen.dedup_by";

/// Marker: committed, not yet ripened.
#[derive(Clone, Copy, Debug)]
pub struct Nectar;

/// Marker: ripened, not yet committed.
#[derive(Clone, Copy, Debug)]
pub struct Ripe;

/// Marker: ripened, checked and sealed.
#[derive(Clone, Copy, Debug)]
pub struct Capped;

/// One data file of a Frame, in ripeness state `S`.
#[derive(Clone, Debug)]
pub struct Cell<S> {
    add: Add,
    /// The sort key the file's rows are ordered by (empty if unsorted).
    sorted_by: Vec<String>,
    _state: PhantomData<S>,
}

impl<S> Cell<S> {
    /// The file's path relative to the table root.
    pub fn path(&self) -> &str {
        &self.add.path
    }

    /// The file's size in bytes.
    pub fn bytes(&self) -> u64 {
        self.add.size.max(0) as u64
    }

    /// The file's row count, from the log's statistics.
    pub fn rows(&self) -> u64 {
        self.add
            .stats
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .and_then(|v| v["numRecords"].as_u64())
            .unwrap_or(0)
    }

    /// When the file was written, in milliseconds since the epoch.
    pub fn modified_ms(&self) -> i64 {
        self.add.modification_time
    }

    /// The file's partition values.
    pub fn partition_values(&self) -> &HashMap<String, Option<String>> {
        &self.add.partition_values
    }

    /// The Delta `add` action behind this Cell.
    pub fn add(&self) -> &Add {
        &self.add
    }

    pub(crate) fn into_add(self) -> Add {
        self.add
    }
}

impl Cell<Nectar> {
    /// A Cell as it stands in the log. `None` if the file is already capped.
    /// Files with no state tag (written by another engine) count as nectar.
    pub fn from_add(add: Add) -> Option<Self> {
        match state_of(&add) {
            Some(CellState::Capped) => None,
            _ => Some(Self {
                add,
                sorted_by: Vec::new(),
                _state: PhantomData,
            }),
        }
    }
}

impl Cell<Ripe> {
    /// A freshly written file, ripened by sorting on `sorted_by`.
    pub(crate) fn ripened(mut add: Add, sorted_by: Vec<String>) -> Self {
        if let Some(tags) = add.tags.as_mut() {
            tags.remove(STATE_TAG);
        }
        Self {
            add,
            sorted_by,
            _state: PhantomData,
        }
    }

    /// Seal the Cell if it passes the ripeness checks; otherwise hand it back
    /// unchanged.
    ///
    /// The capping commit changes no data, so the sealed file is marked
    /// `dataChange = false` and streaming readers skip it.
    pub fn cap(
        self,
        checks: &RipenessChecks,
    ) -> std::result::Result<Cell<Capped>, Box<Cell<Ripe>>> {
        if self.add.size <= 0 || self.rows() == 0 || self.sorted_by != checks.recipe.sort_by {
            return Err(Box::new(self));
        }
        let mut add = self.add;
        add.data_change = false;
        add.tags.get_or_insert_with(HashMap::new).insert(
            STATE_TAG.to_string(),
            Some(CellState::Capped.as_str().into()),
        );
        Ok(Cell {
            add,
            sorted_by: self.sorted_by,
            _state: PhantomData,
        })
    }
}

impl Cell<Capped> {
    /// A capped Cell as it stands in the log.
    pub(crate) fn capped_from_log(add: Add) -> Self {
        Self {
            add,
            sorted_by: Vec::new(),
            _state: PhantomData,
        }
    }
}

/// What a Cell must satisfy before it is capped.
#[derive(Clone, Debug, Default)]
pub struct RipenessChecks {
    /// The recipe the Cell was ripened with: its rows must be in this order.
    pub recipe: Recipe,
}

/// The state recorded in a file's `apiary.state` tag.
pub fn state_of(add: &Add) -> Option<CellState> {
    add.tags
        .as_ref()
        .and_then(|t| t.get(STATE_TAG))
        .and_then(|v| v.as_deref())
        .and_then(CellState::parse)
}

/// How a Frame ripens.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recipe {
    /// Rows are ordered by these columns, ascending, nulls last.
    pub sort_by: Vec<String>,
    /// Rows sharing the values of these columns are one row: the last one
    /// (the most recently arrived) wins.
    pub dedup_by: Vec<String>,
}

impl Recipe {
    /// Whether ripening changes nothing.
    pub fn is_empty(&self) -> bool {
        self.sort_by.is_empty() && self.dedup_by.is_empty()
    }

    /// Read a recipe from a table's properties.
    pub fn from_properties(properties: &HashMap<String, String>) -> Self {
        let list = |key: &str| -> Vec<String> {
            properties
                .get(key)
                .map(|v| {
                    v.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default()
        };
        Self {
            sort_by: list(SORT_BY_PROPERTY),
            dedup_by: list(DEDUP_BY_PROPERTY),
        }
    }

    /// The table properties that record this recipe.
    pub fn to_properties(&self) -> HashMap<String, String> {
        HashMap::from([
            (SORT_BY_PROPERTY.to_string(), self.sort_by.join(",")),
            (DEDUP_BY_PROPERTY.to_string(), self.dedup_by.join(",")),
        ])
    }

    /// Ripen a batch: drop duplicates on the dedup key (the last row wins),
    /// then sort on the sort key. Rows keep their relative order otherwise.
    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.is_empty() || batch.num_rows() == 0 {
            return Ok(batch.clone());
        }
        let mut batch = batch.clone();
        if !self.dedup_by.is_empty() {
            batch = dedup_last(&batch, &self.dedup_by)?;
        }
        if !self.sort_by.is_empty() {
            batch = sort(&batch, &self.sort_by)?;
        }
        Ok(batch)
    }
}

fn columns<'a>(
    batch: &'a RecordBatch,
    names: &[String],
) -> Result<Vec<&'a arrow::array::ArrayRef>> {
    names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|i| batch.column(i))
                .map_err(|_| ApiaryError::Schema {
                    message: format!("Ripening column '{name}' is not in the frame"),
                })
        })
        .collect()
}

fn arrow_err(e: arrow::error::ArrowError) -> ApiaryError {
    ApiaryError::Internal {
        message: format!("Ripening failed: {e}"),
    }
}

fn dedup_last(batch: &RecordBatch, key: &[String]) -> Result<RecordBatch> {
    let cols = columns(batch, key)?;
    let converter = RowConverter::new(
        cols.iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect(),
    )
    .map_err(arrow_err)?;
    let arrays: Vec<_> = cols.into_iter().cloned().collect();
    let rows = converter.convert_columns(&arrays).map_err(arrow_err)?;

    let mut last: HashMap<arrow::row::OwnedRow, u32> = HashMap::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        last.insert(rows.row(i).owned(), i as u32);
    }
    if last.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let mut keep: Vec<u32> = last.into_values().collect();
    keep.sort_unstable();
    take_record_batch(batch, &UInt32Array::from(keep)).map_err(arrow_err)
}

fn sort(batch: &RecordBatch, key: &[String]) -> Result<RecordBatch> {
    let sort_columns: Vec<SortColumn> = columns(batch, key)?
        .into_iter()
        .map(|values| SortColumn {
            values: values.clone(),
            options: Some(arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            }),
        })
        .collect();
    let indices = lexsort_to_indices(&sort_columns, None).map_err(arrow_err)?;
    take_record_batch(batch, &indices).map_err(arrow_err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};

    use super::*;

    fn batch(ids: Vec<i64>, vals: Vec<&str>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(ids)) as _),
            ("val", Arc::new(StringArray::from(vals)) as _),
        ])
        .unwrap()
    }

    fn recipe(sort: &[&str], dedup: &[&str]) -> Recipe {
        Recipe {
            sort_by: sort.iter().map(|s| s.to_string()).collect(),
            dedup_by: dedup.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn properties_round_trip() {
        let r = recipe(&["a", "b"], &["a"]);
        assert_eq!(Recipe::from_properties(&r.to_properties()), r);
        assert!(Recipe::from_properties(&HashMap::new()).is_empty());
    }

    #[test]
    fn sorts_ascending() {
        let out = recipe(&["id"], &[])
            .apply(&batch(vec![3, 1, 2], vec!["c", "a", "b"]))
            .unwrap();
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    #[test]
    fn dedup_keeps_the_last_row() {
        let out = recipe(&[], &["id"])
            .apply(&batch(vec![1, 2, 1, 3, 2], vec!["a", "b", "c", "d", "e"]))
            .unwrap();
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 3, 2]);
        assert_eq!(
            (0..3).map(|i| vals.value(i)).collect::<Vec<_>>(),
            ["c", "d", "e"]
        );
    }

    #[test]
    fn unknown_column_is_an_error() {
        assert!(
            recipe(&["nope"], &[])
                .apply(&batch(vec![1], vec!["a"]))
                .is_err()
        );
    }
}
