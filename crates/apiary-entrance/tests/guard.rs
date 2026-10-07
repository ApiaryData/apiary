//! Guards against a running Node: what lands, what is refused, what is set aside.

mod common;

use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;

use apiary_core::ApiaryError;
use apiary_entrance::{Admission, Source};
use common::{fixture, good, rows, with_unknown_column};
use std::sync::Arc;

fn text_ids() -> RecordBatch {
    RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["not a number"])) as ArrayRef,
    )])
    .unwrap()
}

#[tokio::test]
async fn a_matching_batch_lands_in_the_crop() {
    let f = fixture().await;
    let source = Source::Caller("test".into());
    let admission = f
        .guard
        .admit("farm", "field", "readings", &good(), &source)
        .await
        .unwrap();
    assert!(matches!(admission, Admission::Landed(r) if r.rows == 2));
    assert_eq!(rows(&f.node).await, 2);
    assert!(f.guard.set_aside().list().unwrap().is_empty());
}

#[tokio::test]
async fn a_caller_is_told_why_and_nothing_lands() {
    let f = fixture().await;
    let source = Source::Caller("test".into());

    let err = f
        .guard
        .admit("farm", "field", "readings", &with_unknown_column(), &source)
        .await
        .expect_err("an unknown column is refused");
    assert!(matches!(&err, ApiaryError::Schema { message } if message.contains("humidity")));

    let err = f
        .guard
        .admit("farm", "field", "readings", &text_ids(), &source)
        .await
        .expect_err("values that do not fit are refused");
    assert!(matches!(err, ApiaryError::Schema { .. }));

    assert_eq!(rows(&f.node).await, 0);
    assert!(
        f.guard.set_aside().list().unwrap().is_empty(),
        "a caller keeps its data, so nothing is set aside"
    );
}

#[tokio::test]
async fn a_stream_deposit_is_set_aside_with_its_reason() {
    let f = fixture().await;
    let source = Source::Stream("topic plant/line1".into());

    let admission = f
        .guard
        .admit("farm", "field", "readings", &with_unknown_column(), &source)
        .await
        .unwrap();
    let Admission::SetAside(record) = admission else {
        panic!("expected the deposit to be set aside");
    };
    assert_eq!(record.frame, "farm.field.readings");
    assert_eq!(record.source, "topic plant/line1");
    assert!(record.reason.contains("humidity"), "{}", record.reason);
    assert_eq!(record.rows, 1);

    let admission = f
        .guard
        .admit("farm", "field", "readings", &text_ids(), &source)
        .await
        .unwrap();
    assert!(matches!(admission, Admission::SetAside(_)));

    assert_eq!(rows(&f.node).await, 0, "nothing reached the crop");
    assert_eq!(f.guard.set_aside().list().unwrap().len(), 2);
}

#[tokio::test]
async fn a_missing_frame_is_an_error_not_a_refusal() {
    let f = fixture().await;
    let source = Source::Stream("topic".into());
    let err = f
        .guard
        .admit("farm", "field", "nope", &good(), &source)
        .await
        .expect_err("no such frame");
    assert!(matches!(err, ApiaryError::EntityNotFound { .. }), "{err}");
    assert!(f.guard.set_aside().list().unwrap().is_empty());
}

#[tokio::test]
async fn an_oversized_batch_is_refused() {
    let f = fixture().await;
    let guard = f.guard.clone().with_max_batch_bytes(8);
    let source = Source::Caller("test".into());
    let err = guard
        .admit("farm", "field", "readings", &good(), &source)
        .await
        .expect_err("too big");
    assert!(matches!(err, ApiaryError::Schema { .. }));
    assert_eq!(rows(&f.node).await, 0);
}
