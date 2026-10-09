//! Tests for capping, harvest, retirement and clearing, on real Delta tables.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use apiary_core::{FieldDef, FrameSchema};

use crate::cell::Recipe;
use crate::comb::{CellState, Comb, STATE_TAG};
use crate::upkeep::CapOptions;

const CELL: u64 = 64 * 1024 * 1024;

fn schema() -> FrameSchema {
    let field = |name: &str, ty: &str| FieldDef {
        name: name.into(),
        data_type: ty.into(),
        nullable: false,
    };
    FrameSchema {
        fields: vec![
            field("region", "string"),
            field("sensor", "int64"),
            field("temp", "float64"),
        ],
    }
}

fn batch(region: &str, sensors: Vec<i64>, temp: f64) -> RecordBatch {
    let n = sensors.len();
    RecordBatch::try_from_iter(vec![
        (
            "region",
            Arc::new(StringArray::from(vec![region; n])) as ArrayRef,
        ),
        ("sensor", Arc::new(Int64Array::from(sensors)) as ArrayRef),
        (
            "temp",
            Arc::new(Float64Array::from(vec![temp; n])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn recipe(sort: &[&str], dedup: &[&str]) -> Recipe {
    Recipe {
        sort_by: sort.iter().map(|s| s.to_string()).collect(),
        dedup_by: dedup.iter().map(|s| s.to_string()).collect(),
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Cap whatever is nectar, however small or young.
fn cap_now() -> CapOptions {
    CapOptions {
        target_cell_size: CELL,
        max_age: Duration::ZERO,
        now_ms: now_ms(),
        max_commits: None,
    }
}

async fn setup(partition_by: &[String]) -> (TempDir, Comb, deltalake::DeltaTable) {
    let dir = TempDir::new().unwrap();
    let comb = Comb::from_local_path(dir.path()).unwrap();
    let table = comb
        .create_frame_table("h", "b", "f", &schema(), partition_by)
        .await
        .unwrap();
    (dir, comb, table)
}

async fn reopen(comb: &Comb) -> deltalake::DeltaTable {
    comb.open_frame_table("h", "b", "f").await.unwrap().unwrap()
}

fn sensors(batch: &RecordBatch) -> Vec<i64> {
    let column = batch.column_by_name("sensor").unwrap();
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn states(table: &deltalake::DeltaTable) -> Vec<Option<String>> {
    #[allow(deprecated)]
    table
        .snapshot()
        .unwrap()
        .log_data()
        .into_iter()
        .map(|f| {
            f.add_action()
                .tags
                .and_then(|t| t.get(STATE_TAG).cloned().flatten())
        })
        .collect()
}

fn log_text(dir: &TempDir, last_version: u64) -> String {
    let path = dir
        .path()
        .join("h/b/f/_delta_log")
        .join(format!("{last_version:020}.json"));
    std::fs::read_to_string(path).unwrap()
}

#[tokio::test]
async fn recipe_is_stored_in_table_properties_and_checked() {
    let (_dir, comb, table) = setup(&[]).await;
    assert!(comb.recipe(&table).unwrap().is_empty());

    let table = comb
        .set_recipe(&table, &recipe(&["sensor"], &["sensor"]))
        .await
        .unwrap();
    assert_eq!(
        comb.recipe(&reopen(&comb).await).unwrap(),
        recipe(&["sensor"], &["sensor"])
    );

    let err = comb.set_recipe(&table, &recipe(&["nope"], &[])).await;
    assert!(err.is_err(), "a recipe naming a missing column is refused");
}

#[tokio::test]
async fn deposit_ripens_with_the_recipe() {
    let (_dir, comb, table) = setup(&[]).await;
    let table = comb
        .set_recipe(&table, &recipe(&["sensor"], &["sensor"]))
        .await
        .unwrap();

    let rows = batch("north", vec![3, 1, 3, 2, 1], 1.0);
    let committed = comb
        .deposit(&table, &rows, CELL, "crop-a", 1)
        .await
        .unwrap();
    assert_eq!(committed.rows, 3, "duplicates on the key collapse");

    let read = comb
        .read(&reopen(&comb).await, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sensors(&read),
        vec![1, 2, 3],
        "and the rows come out sorted"
    );
}

#[tokio::test]
async fn capping_merges_small_nectar_into_one_sorted_capped_cell() {
    let (dir, comb, table) = setup(&[]).await;
    let table = comb
        .set_recipe(&table, &recipe(&["sensor"], &[]))
        .await
        .unwrap();
    for sensors in [vec![9, 7], vec![8, 1], vec![5, 6]] {
        let t = reopen(&comb).await;
        comb.append(&t, &batch("north", sensors, 1.0), CELL, CellState::Nectar)
            .await
            .unwrap();
    }
    let table = {
        drop(table);
        reopen(&comb).await
    };
    assert_eq!(states(&table).len(), 3);

    let report = comb.cap(&table, &cap_now()).await.unwrap();
    assert_eq!(report.nectar_cells, 3);
    assert_eq!(report.capped_cells, 1);
    assert_eq!(report.rows, 6);
    assert_eq!(report.aborted, 0);

    let after = reopen(&comb).await;
    assert_eq!(states(&after), vec![Some("capped".to_string())]);
    let read = comb.read(&after, None).await.unwrap().unwrap();
    assert_eq!(sensors(&read), vec![1, 5, 6, 7, 8, 9], "merged and sorted");

    let text = log_text(&dir, after.version().unwrap() as u64);
    assert!(
        text.contains("\"dataChange\":false") && !text.contains("\"dataChange\":true"),
        "a capping commit changes no data: {text}"
    );

    let again = comb.cap(&after, &cap_now()).await.unwrap();
    assert_eq!(
        again.nectar_cells, 0,
        "capped Cells are never touched again"
    );
}

#[tokio::test]
async fn capping_waits_for_a_group_to_fill_unless_it_is_old() {
    let (_dir, comb, table) = setup(&[]).await;
    comb.append(
        &table,
        &batch("north", vec![1], 1.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();
    let table = reopen(&comb).await;

    let young = CapOptions {
        target_cell_size: CELL,
        max_age: Duration::from_secs(600),
        now_ms: now_ms(),
        max_commits: None,
    };
    assert_eq!(comb.cap(&table, &young).await.unwrap().nectar_cells, 0);

    let old = CapOptions {
        now_ms: now_ms() + 601_000,
        ..young
    };
    assert_eq!(comb.cap(&table, &old).await.unwrap().nectar_cells, 1);
}

#[tokio::test]
async fn capping_keeps_partitions_apart() {
    let (_dir, comb, table) = setup(&["region".to_string()]).await;
    for (region, sensors) in [("north", vec![2]), ("south", vec![3]), ("north", vec![1])] {
        let t = reopen(&comb).await;
        comb.append(&t, &batch(region, sensors, 1.0), CELL, CellState::Nectar)
            .await
            .unwrap();
    }
    let table = {
        drop(table);
        reopen(&comb).await
    };

    let report = comb.cap(&table, &cap_now()).await.unwrap();
    assert_eq!(report.nectar_cells, 3);
    assert_eq!(report.capped_cells, 2, "one capped Cell per partition");

    let after = reopen(&comb).await;
    let mut north = std::collections::HashMap::new();
    north.insert("region".to_string(), "north".to_string());
    let read = comb.read(&after, Some(&north)).await.unwrap().unwrap();
    assert_eq!(read.num_rows(), 2);
    let regions = arrow::compute::cast(
        read.column_by_name("region").unwrap(),
        &arrow::datatypes::DataType::Utf8,
    )
    .unwrap();
    let regions = regions.as_any().downcast_ref::<StringArray>().unwrap();
    assert!((0..read.num_rows()).all(|i| regions.value(i) == "north"));
}

#[tokio::test]
async fn capping_yields_to_a_concurrent_overwrite() {
    let (_dir, comb, table) = setup(&[]).await;
    for sensors in [vec![1], vec![2]] {
        let t = reopen(&comb).await;
        comb.append(&t, &batch("north", sensors, 1.0), CELL, CellState::Nectar)
            .await
            .unwrap();
    }
    drop(table);
    // The Ripener looks at the table...
    let stale = reopen(&comb).await;
    // ...and before it commits, a user replaces all the data.
    let current = reopen(&comb).await;
    comb.overwrite(
        &current,
        &batch("north", vec![42], 2.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();

    let report = comb.cap(&stale, &cap_now()).await.unwrap();
    assert_eq!(report.aborted, 1, "ripening yields to the user's write");
    assert_eq!(report.capped_cells, 0);

    let read = comb
        .read(&reopen(&comb).await, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sensors(&read), vec![42], "the user's data stands");
}

#[tokio::test]
async fn harvest_copies_only_capped_cells_oldest_first_and_only_once() {
    let (_dir, comb, table) = setup(&[]).await;
    let harvest_dir = TempDir::new().unwrap();
    let harvest = Comb::from_local_path(harvest_dir.path()).unwrap();
    let names = ("h", "b", "f");

    // One capped Cell, then one nectar Cell.
    comb.append(
        &table,
        &batch("north", vec![1, 2], 1.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();
    comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();
    comb.append(
        &reopen(&comb).await,
        &batch("north", vec![3], 1.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();

    let site = reopen(&comb).await;
    let first = comb
        .harvest(&site, &harvest, names, &schema(), &[], u64::MAX)
        .await
        .unwrap();
    assert_eq!(first.cells, 1, "the nectar is not harvested");

    let target = harvest
        .open_frame_table("h", "b", "f")
        .await
        .unwrap()
        .unwrap();
    let read = harvest.read(&target, None).await.unwrap().unwrap();
    let mut got = sensors(&read);
    got.sort();
    assert_eq!(got, vec![1, 2]);
    assert_eq!(states(&target), vec![Some("capped".to_string())]);

    let again = comb
        .harvest(&site, &harvest, names, &schema(), &[], u64::MAX)
        .await
        .unwrap();
    assert_eq!(again.cells, 0, "a harvested Cell is not copied twice");

    // Cap the rest and harvest it: the older Cell stays put.
    comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();
    let site = reopen(&comb).await;
    let second = comb
        .harvest(&site, &harvest, names, &schema(), &[], u64::MAX)
        .await
        .unwrap();
    assert_eq!(second.cells, 1);
    let target = harvest
        .open_frame_table("h", "b", "f")
        .await
        .unwrap()
        .unwrap();
    let read = harvest.read(&target, None).await.unwrap().unwrap();
    let mut got = sensors(&read);
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

#[tokio::test]
async fn harvest_respects_its_byte_budget() {
    let (_dir, comb, table) = setup(&[]).await;
    let harvest_dir = TempDir::new().unwrap();
    let harvest = Comb::from_local_path(harvest_dir.path()).unwrap();

    // Two capped Cells, capped separately so they stay two files.
    comb.append(
        &table,
        &batch("north", vec![1], 1.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();
    comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();
    comb.append(
        &reopen(&comb).await,
        &batch("north", vec![2], 1.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();
    comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();

    let site = reopen(&comb).await;
    let pass = comb
        .harvest(&site, &harvest, ("h", "b", "f"), &schema(), &[], 1)
        .await
        .unwrap();
    assert_eq!(pass.cells, 1, "a tiny budget still moves one Cell");
    assert_eq!(pass.remaining, 1);

    let target = harvest
        .open_frame_table("h", "b", "f")
        .await
        .unwrap()
        .unwrap();
    let read = harvest.read(&target, None).await.unwrap().unwrap();
    assert_eq!(sensors(&read), vec![1], "the oldest goes first");
}

#[tokio::test]
async fn only_harvested_cells_are_retired_and_only_when_old_enough() {
    let (_dir, comb, table) = setup(&[]).await;
    let harvest_dir = TempDir::new().unwrap();
    let harvest = Comb::from_local_path(harvest_dir.path()).unwrap();

    comb.append(
        &table,
        &batch("north", vec![1], 1.0),
        CELL,
        CellState::Nectar,
    )
    .await
    .unwrap();
    comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();
    let site = reopen(&comb).await;

    // Not harvested yet: nothing goes, however old.
    let empty = harvest
        .open_or_create_frame_table("h", "b", "f", &schema(), &[])
        .await
        .unwrap();
    let far_future = now_ms() + 10 * 365 * 86_400_000;
    let none = comb
        .retire_harvested(&site, &empty, Duration::ZERO, far_future)
        .await
        .unwrap();
    assert_eq!(none, 0, "a Cell that is not harvested never leaves");

    comb.harvest(&site, &harvest, ("h", "b", "f"), &schema(), &[], u64::MAX)
        .await
        .unwrap();
    let target = harvest
        .open_frame_table("h", "b", "f")
        .await
        .unwrap()
        .unwrap();

    let young = comb
        .retire_harvested(&site, &target, Duration::from_secs(3600), now_ms())
        .await
        .unwrap();
    assert_eq!(young, 0, "inside the retention window it stays");

    let old = comb
        .retire_harvested(&site, &target, Duration::from_secs(3600), far_future)
        .await
        .unwrap();
    assert_eq!(old, 1);
    assert_eq!(comb.frame_stats(&reopen(&comb).await).unwrap().cells, 0);
    assert_eq!(
        harvest.frame_stats(&target).unwrap().cells,
        1,
        "the harvest keeps it"
    );
}

#[tokio::test]
async fn clearing_deletes_files_capping_removed() {
    let (dir, comb, table) = setup(&[]).await;
    for sensors in [vec![1], vec![2]] {
        let t = reopen(&comb).await;
        comb.append(&t, &batch("north", sensors, 1.0), CELL, CellState::Nectar)
            .await
            .unwrap();
    }
    drop(table);
    comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();

    let parquet = |dir: &TempDir| {
        std::fs::read_dir(dir.path().join("h/b/f"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".parquet")
            })
            .count()
    };
    assert_eq!(parquet(&dir), 3, "two nectar files and the capped one");

    let table = reopen(&comb).await;
    // Inside the grace period nothing is deleted.
    assert_eq!(
        comb.clear(&table, Duration::from_secs(3600)).await.unwrap(),
        0
    );
    assert_eq!(parquet(&dir), 3);

    let cleared = comb.clear(&table, Duration::ZERO).await.unwrap();
    assert_eq!(cleared, 2);
    assert_eq!(parquet(&dir), 1);
    let read = comb
        .read(&reopen(&comb).await, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sensors(&read), vec![1, 2], "the table reads as before");
}

#[tokio::test]
async fn an_overwrite_built_before_capping_cannot_leave_the_capped_copy_behind() {
    let (_dir, comb, table) = setup(&[]).await;
    for sensors in [vec![1], vec![2]] {
        let t = reopen(&comb).await;
        comb.append(&t, &batch("north", sensors, 1.0), CELL, CellState::Nectar)
            .await
            .unwrap();
    }
    drop(table);
    // The user's overwrite is built from the table as it is now...
    let before_capping = reopen(&comb).await;
    // ...capping commits first...
    let report = comb.cap(&reopen(&comb).await, &cap_now()).await.unwrap();
    assert_eq!(report.capped_cells, 1);

    // ...and the overwrite must not slip in after it: Delta treats the capping
    // commit as changing no data, so only the rewrite fence stops the capped copy
    // of the old rows from outliving the overwrite.
    let refused = comb
        .overwrite(
            &before_capping,
            &batch("north", vec![42], 2.0),
            CELL,
            CellState::Nectar,
        )
        .await;
    assert!(
        refused.is_err(),
        "the overwrite conflicts with the capping commit"
    );

    let read = comb
        .read(&reopen(&comb).await, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sensors(&read), vec![1, 2], "nothing was half-replaced");
}
