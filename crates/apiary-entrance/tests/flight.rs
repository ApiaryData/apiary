//! The Flight SQL entrance, driven by a real Flight SQL client over gRPC.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetTables, CommandStatementIngest};
use futures::TryStreamExt;
use tonic::transport::Channel;

use apiary_entrance::flight;
use common::{Fixture, fixture, good, rows, with_unknown_column};

async fn serve(f: &Fixture, token: Option<&str>) -> (flight::RunningFlight, SocketAddr) {
    let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = flight::start(f.guard.clone(), listen, token.map(String::from))
        .await
        .unwrap();
    let addr = server.addr();
    (server, addr)
}

async fn client(addr: SocketAddr) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    FlightSqlServiceClient::new(channel)
}

fn ingest_command(table: &str) -> CommandStatementIngest {
    CommandStatementIngest {
        table: table.to_string(),
        schema: Some("field".to_string()),
        catalog: Some("farm".to_string()),
        ..Default::default()
    }
}

async fn ingest(
    client: &mut FlightSqlServiceClient<Channel>,
    table: &str,
    batch: RecordBatch,
) -> Result<i64, arrow_flight::error::FlightError> {
    client
        .execute_ingest(
            ingest_command(table),
            futures::stream::iter(vec![Ok(batch)]),
        )
        .await
}

async fn query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> (arrow::datatypes::SchemaRef, Vec<RecordBatch>) {
    let info = client.execute(sql.to_string(), None).await.unwrap();
    let schema = Arc::new(info.clone().try_decode_schema().unwrap());
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    (schema, batches)
}

#[tokio::test]
async fn a_deposit_over_flight_is_queryable_over_flight() {
    let f = fixture().await;
    let (server, addr) = serve(&f, None).await;
    let mut client = client(addr).await;

    let landed = ingest(&mut client, "readings", good()).await.unwrap();
    assert_eq!(landed, 2);
    assert_eq!(rows(&f.node).await, 2);

    let (schema, batches) = query(
        &mut client,
        "SELECT id, _stage FROM farm.field.readings ORDER BY id",
    )
    .await;
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
    let stage = batches[0]
        .column_by_name("_stage")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        stage.value(0),
        "crop",
        "ingested rows are flagged as unshipped"
    );

    // The rows read from each stage travel in the result's schema.
    assert_eq!(
        schema
            .metadata()
            .get("apiary.rows.crop")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        schema
            .metadata()
            .get("apiary.rows.comb")
            .map(String::as_str),
        Some("0")
    );

    server.stop().await;
}

#[tokio::test]
async fn a_query_with_no_rows_still_has_its_columns() {
    let f = fixture().await;
    let (server, addr) = serve(&f, None).await;
    let mut client = client(addr).await;
    ingest(&mut client, "readings", good()).await.unwrap();

    let (schema, batches) = query(
        &mut client,
        "SELECT id, temp FROM farm.field.readings WHERE id > 100",
    )
    .await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "temp"]);

    server.stop().await;
}

#[tokio::test]
async fn a_batch_that_does_not_fit_is_refused_with_the_reason() {
    let f = fixture().await;
    let (server, addr) = serve(&f, None).await;
    let mut client = client(addr).await;

    let err = ingest(&mut client, "readings", with_unknown_column())
        .await
        .expect_err("an unknown column is refused");
    assert!(err.to_string().contains("humidity"), "{err}");
    assert_eq!(rows(&f.node).await, 0);

    let err = ingest(&mut client, "nope", good())
        .await
        .expect_err("no such frame");
    assert!(
        err.to_string().to_lowercase().contains("not found"),
        "{err}"
    );

    server.stop().await;
}

#[tokio::test]
async fn a_result_can_be_fetched_once() {
    let f = fixture().await;
    let (server, addr) = serve(&f, None).await;
    let mut client = client(addr).await;

    let info = client
        .execute("SHOW HIVES".to_string(), None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let first: Vec<RecordBatch> = client
        .do_get(ticket.clone())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!first.is_empty());
    assert!(client.do_get(ticket).await.is_err(), "the result is gone");

    server.stop().await;
}

#[tokio::test]
async fn a_bearer_token_is_required_when_one_is_set() {
    let f = fixture().await;
    let (server, addr) = serve(&f, Some("s3cret")).await;

    let mut anonymous = client(addr).await;
    assert!(
        anonymous
            .execute("SHOW HIVES".to_string(), None)
            .await
            .is_err()
    );

    let mut wrong = client(addr).await;
    wrong.set_token("nope".to_string());
    assert!(wrong.execute("SHOW HIVES".to_string(), None).await.is_err());

    let mut right = client(addr).await;
    right.set_token("s3cret".to_string());
    assert!(right.execute("SHOW HIVES".to_string(), None).await.is_ok());

    server.stop().await;
}

#[tokio::test]
async fn the_registry_is_browsable_as_catalogs_and_tables() {
    let f = fixture().await;
    let (server, addr) = serve(&f, None).await;
    let mut client = client(addr).await;

    let info = client.get_catalogs().await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let catalogs = batches[0]
        .column_by_name("catalog_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(catalogs.value(0), "farm");

    let info = client
        .get_tables(CommandGetTables {
            include_schema: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let names = batches[0]
        .column_by_name("table_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "readings");
    assert_eq!(batches[0].num_rows(), 1);

    let info = client.get_sql_info(vec![]).await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(batches[0].num_rows() > 0);

    server.stop().await;
}
