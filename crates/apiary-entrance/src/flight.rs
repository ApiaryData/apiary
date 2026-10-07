//! The Flight SQL entrance: queries and deposits over gRPC.
//!
//! Beekeepers, BI tools and the Python client reach a Node here. A query is a
//! Flight SQL statement; a deposit is a Flight SQL bulk ingest, where the
//! catalog is the Hive, the schema is the Box and the table is the Frame.
//! Deposits pass through the [`Guard`], so a batch that does not fit its Frame
//! is refused with the reason and nothing lands.
//!
//! A query runs when its `FlightInfo` is requested, because that is where the
//! result schema has to come from, and the result waits (briefly, and once) for
//! the `DoGet` that reads it. Metadata calls (`GetCatalogs`, `GetDbSchemas`,
//! `GetTables`, `GetSqlInfo`) answer from the registry.
//!
//! There is no user authentication yet: the server can require one shared bearer
//! token, and by default it listens on the loopback address only.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::metadata::{
    GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder, SqlInfoData, SqlInfoDataBuilder,
};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTables,
    CommandStatementIngest, CommandStatementQuery, ProstMessageExt, SqlInfo, TableExistsOption,
    TableNotExistOption, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket, decode::FlightRecordBatchStream,
};
use futures::{Stream, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use apiary_comb::STAGE_COLUMN;
use apiary_core::{ApiaryError, Result};

use crate::guard::{Admission, Guard, Source};

/// How long a query's result waits for its `DoGet`.
const RESULT_TTL: Duration = Duration::from_secs(60);

/// The most results held at once; the oldest is dropped beyond this.
const MAX_PENDING: usize = 64;

type BoxedStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

/// A query result waiting to be fetched.
struct Pending {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    created: Instant,
}

/// The Flight SQL service for one Node.
pub struct FlightEntrance {
    guard: Guard,
    pending: Mutex<HashMap<Vec<u8>, Pending>>,
}

impl FlightEntrance {
    /// A service admitting through `guard`.
    pub fn new(guard: Guard) -> Self {
        Self {
            guard,
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn stash(&self, batches: Vec<RecordBatch>, schema: SchemaRef) -> Vec<u8> {
        let handle = uuid::Uuid::new_v4().as_bytes().to_vec();
        let mut pending = self.pending.lock().expect("pending results poisoned");
        pending.retain(|_, p| p.created.elapsed() < RESULT_TTL);
        while pending.len() >= MAX_PENDING {
            let oldest = pending
                .iter()
                .min_by_key(|(_, p)| p.created)
                .map(|(k, _)| k.clone());
            match oldest {
                Some(key) => {
                    pending.remove(&key);
                }
                None => break,
            }
        }
        pending.insert(
            handle.clone(),
            Pending {
                batches,
                schema,
                created: Instant::now(),
            },
        );
        handle
    }

    fn take(&self, handle: &[u8]) -> Option<Pending> {
        self.pending
            .lock()
            .expect("pending results poisoned")
            .remove(handle)
    }
}

fn status(e: ApiaryError) -> Status {
    match &e {
        ApiaryError::EntityNotFound { .. } => Status::not_found(e.to_string()),
        ApiaryError::Schema { .. }
        | ApiaryError::Resolution { .. }
        | ApiaryError::Unsupported { .. } => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

fn encode_stream(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> BoxedStream<arrow_flight::FlightData> {
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(batches.into_iter().map(Ok)))
        .map_err(Status::from);
    Box::pin(encoded)
}

fn sql_info() -> &'static SqlInfoData {
    static DATA: OnceLock<SqlInfoData> = OnceLock::new();
    DATA.get_or_init(|| {
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "Apiary");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
        builder.append(SqlInfo::FlightSqlServerReadOnly, false);
        builder.append(SqlInfo::FlightSqlServerSql, true);
        builder.build().expect("static SQL info is valid")
    })
}

#[tonic::async_trait]
impl FlightSqlService for FlightEntrance {
    type FlightService = FlightEntrance;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let node = Arc::clone(self.guard.node());
        let sql = query.query.clone();
        let output = self
            .guard
            .on_cpu(async move { node.sql_with_stages(&sql).await })
            .await
            .map_err(status)?;
        let rows: usize = output.batches.iter().map(RecordBatch::num_rows).sum();
        let handle = self.stash(output.batches, Arc::clone(&output.schema));

        let ticket = TicketStatementQuery {
            statement_handle: handle.into(),
        };
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(&output.schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint)
            .with_total_records(rows as i64)
            .with_total_bytes(-1);
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<BoxedStream<arrow_flight::FlightData>>, Status> {
        let pending = self.take(&ticket.statement_handle).ok_or_else(|| {
            Status::not_found("The result is gone: it was fetched already or it expired")
        })?;
        Ok(Response::new(encode_stream(
            pending.schema,
            pending.batches,
        )))
    }

    async fn do_put_statement_ingest(
        &self,
        command: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> std::result::Result<i64, Status> {
        let (Some(hive), Some(box_name)) = (command.catalog.as_deref(), command.schema.as_deref())
        else {
            return Err(Status::invalid_argument(
                "A deposit names its frame as catalog (the hive), db_schema (the box) and table (the frame)",
            ));
        };
        if command.table.is_empty() {
            return Err(Status::invalid_argument("The deposit names no table"));
        }
        if let Some(options) = &command.table_definition_options {
            let exists_ok = matches!(
                TableExistsOption::try_from(options.if_exists),
                Ok(TableExistsOption::Unspecified | TableExistsOption::Append)
            );
            let missing_ok = matches!(
                TableNotExistOption::try_from(options.if_not_exist),
                Ok(TableNotExistOption::Unspecified | TableNotExistOption::Fail)
            );
            if !exists_ok {
                return Err(Status::unimplemented(
                    "Deposits only append to an existing frame",
                ));
            }
            if !missing_ok {
                return Err(Status::unimplemented(
                    "Frames are created through the registry, not by a deposit",
                ));
            }
        }

        let source = Source::Caller(
            request
                .remote_addr()
                .map_or_else(|| "flight client".to_string(), |a| format!("flight {a}")),
        );
        let mut batches = FlightRecordBatchStream::new_from_flight_data(
            request
                .into_inner()
                .map_err(arrow_flight::error::FlightError::from),
        );
        let mut rows = 0i64;
        while let Some(batch) = batches.try_next().await.map_err(Status::from)? {
            match self
                .guard
                .admit(hive, box_name, &command.table, &batch, &source)
                .await
                .map_err(status)?
            {
                Admission::Landed(result) => rows += result.rows as i64,
                // A caller is never set aside: it was refused above.
                Admission::SetAside(_) => {}
            }
        }
        Ok(rows)
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder(sql_info()).schema();
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(query.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<BoxedStream<arrow_flight::FlightData>>, Status> {
        let builder = query.into_builder(sql_info());
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(encode_stream(schema, vec![batch])))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let schema = query.into_builder().schema();
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(query.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<BoxedStream<arrow_flight::FlightData>>, Status> {
        let mut builder = GetCatalogsBuilder::from(query);
        for hive in self.hives().await? {
            builder.append(hive);
        }
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(encode_stream(schema, vec![batch])))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(query.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<BoxedStream<arrow_flight::FlightData>>, Status> {
        let mut builder = GetDbSchemasBuilder::from(query);
        for (hive, box_name, _) in self.frames().await? {
            builder.append(hive, box_name);
        }
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(encode_stream(schema, vec![batch])))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let schema = query.clone().into_builder().schema();
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(query.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint);
        Ok(Response::new(info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<BoxedStream<arrow_flight::FlightData>>, Status> {
        let include_schema = query.include_schema;
        let mut builder = GetTablesBuilder::from(query);
        for (hive, box_name, frame) in self.frames().await? {
            let schema = if include_schema {
                let stored = self
                    .guard
                    .node()
                    .frame_schema(&hive, &box_name, &frame)
                    .await
                    .map_err(status)?;
                // `SELECT *` returns the virtual stage column last.
                let mut fields: Vec<Arc<Field>> = stored.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new(STAGE_COLUMN, DataType::Utf8, false)));
                Schema::new(fields)
            } else {
                Schema::empty()
            };
            builder
                .append(&hive, &box_name, &frame, "TABLE", &schema)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(encode_stream(schema, vec![batch])))
    }

    async fn list_custom_actions(
        &self,
    ) -> Option<Vec<std::result::Result<arrow_flight::ActionType, Status>>> {
        let describe = |kind: &str, description: &str| {
            Ok(arrow_flight::ActionType {
                r#type: kind.to_string(),
                description: description.to_string(),
            })
        };
        Some(vec![
            describe(ACTION_CREATE_HIVE, "Create a hive: {\"name\"}"),
            describe(ACTION_CREATE_BOX, "Create a box: {\"hive\", \"name\"}"),
            describe(
                ACTION_CREATE_FRAME,
                "Create a frame: {\"hive\", \"box\", \"name\", \"schema\": {column: type}, \"partition_by\": []}",
            ),
            describe(
                ACTION_SET_RECIPE,
                "Set how a frame ripens: {\"hive\", \"box\", \"frame\", \"sort_by\": [], \"dedup_by\": []}",
            ),
            describe(
                ACTION_FLUSH_CROP,
                "Deposit the node's crop into the comb now",
            ),
        ])
    }

    async fn do_action_fallback(
        &self,
        request: Request<arrow_flight::Action>,
    ) -> std::result::Result<Response<BoxedStream<arrow_flight::Result>>, Status> {
        let action = request.into_inner();
        let reply = self.run_action(&action.r#type, &action.body).await?;
        let body = serde_json::to_vec(&reply).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
            arrow_flight::Result { body: body.into() },
        )]))))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Custom action types, for what Flight SQL has no verb for.
pub const ACTION_CREATE_HIVE: &str = "apiary.create_hive";
/// Create a box.
pub const ACTION_CREATE_BOX: &str = "apiary.create_box";
/// Create a frame.
pub const ACTION_CREATE_FRAME: &str = "apiary.create_frame";
/// Set a frame's ripening recipe.
pub const ACTION_SET_RECIPE: &str = "apiary.set_recipe";
/// Deposit the crop now.
pub const ACTION_FLUSH_CROP: &str = "apiary.flush_crop";

#[derive(serde::Deserialize)]
struct CreateHive {
    name: String,
}

#[derive(serde::Deserialize)]
struct CreateBox {
    hive: String,
    name: String,
}

#[derive(serde::Deserialize)]
struct CreateFrame {
    hive: String,
    #[serde(rename = "box")]
    box_name: String,
    name: String,
    schema: serde_json::Value,
    #[serde(default)]
    partition_by: Vec<String>,
}

#[derive(serde::Deserialize)]
struct SetRecipe {
    hive: String,
    #[serde(rename = "box")]
    box_name: String,
    frame: String,
    #[serde(default)]
    sort_by: Vec<String>,
    #[serde(default)]
    dedup_by: Vec<String>,
}

fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> std::result::Result<T, Status> {
    serde_json::from_slice(body)
        .map_err(|e| Status::invalid_argument(format!("The action body is not valid: {e}")))
}

impl FlightEntrance {
    async fn run_action(
        &self,
        kind: &str,
        body: &[u8],
    ) -> std::result::Result<serde_json::Value, Status> {
        let node = self.guard.node();
        match kind {
            ACTION_CREATE_HIVE => {
                let request: CreateHive = parse(body)?;
                node.registry
                    .create_hive(&request.name)
                    .await
                    .map_err(status)?;
                Ok(serde_json::json!({}))
            }
            ACTION_CREATE_BOX => {
                let request: CreateBox = parse(body)?;
                node.registry
                    .create_box(&request.hive, &request.name)
                    .await
                    .map_err(status)?;
                Ok(serde_json::json!({}))
            }
            ACTION_CREATE_FRAME => {
                let request: CreateFrame = parse(body)?;
                node.registry
                    .create_frame(
                        &request.hive,
                        &request.box_name,
                        &request.name,
                        request.schema,
                        request.partition_by,
                    )
                    .await
                    .map_err(status)?;
                node.init_frame_table(&request.hive, &request.box_name, &request.name)
                    .await
                    .map_err(status)?;
                Ok(serde_json::json!({}))
            }
            ACTION_SET_RECIPE => {
                let request: SetRecipe = parse(body)?;
                node.set_recipe(
                    &request.hive,
                    &request.box_name,
                    &request.frame,
                    request.sort_by,
                    request.dedup_by,
                )
                .await
                .map_err(status)?;
                Ok(serde_json::json!({}))
            }
            ACTION_FLUSH_CROP => {
                let report = node.flush_crop().await.map_err(status)?;
                Ok(serde_json::json!({
                    "frames": report.frames,
                    "segments": report.segments,
                    "rows": report.rows,
                }))
            }
            other => Err(Status::invalid_argument(format!(
                "Unknown action '{other}'"
            ))),
        }
    }

    async fn hives(&self) -> std::result::Result<Vec<String>, Status> {
        let mut hives = self
            .guard
            .node()
            .registry
            .list_hives()
            .await
            .map_err(status)?;
        hives.sort();
        Ok(hives)
    }

    /// Every Frame as (hive, box, frame), sorted.
    async fn frames(&self) -> std::result::Result<Vec<(String, String, String)>, Status> {
        let registry = self
            .guard
            .node()
            .registry
            .load_or_create()
            .await
            .map_err(status)?;
        let mut frames = Vec::new();
        for (hive_name, hive) in &registry.hives {
            for (box_name, box_) in &hive.boxes {
                for frame_name in box_.frames.keys() {
                    frames.push((hive_name.clone(), box_name.clone(), frame_name.clone()));
                }
            }
        }
        frames.sort();
        Ok(frames)
    }
}

/// A running Flight SQL server.
pub struct RunningFlight {
    addr: SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningFlight {
    /// The address the server is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop serving and wait for the server to finish.
    pub async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

/// Start serving Flight SQL for `guard`'s Node on `listen` (port 0 picks one).
/// With a `token`, every call must carry `authorization: Bearer <token>`.
pub async fn start(
    guard: Guard,
    listen: SocketAddr,
    token: Option<String>,
) -> Result<RunningFlight> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| ApiaryError::storage(format!("Failed to listen on {listen}"), e))?;
    let addr = listener
        .local_addr()
        .map_err(|e| ApiaryError::storage("Failed to read the listening address", e))?;

    let service = FlightServiceServer::new(FlightEntrance::new(guard));
    let expected = token.map(|t| format!("Bearer {t}"));
    let intercepted = tonic::service::interceptor::InterceptedService::new(
        service,
        move |request: Request<()>| -> std::result::Result<Request<()>, Status> {
            let Some(expected) = &expected else {
                return Ok(request);
            };
            match request.metadata().get("authorization").map(|v| v.to_str()) {
                Some(Ok(given)) if given == expected => Ok(request),
                _ => Err(Status::unauthenticated("A bearer token is required")),
            }
        },
    );

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let task = tokio::spawn(async move {
        let served = tonic::transport::Server::builder()
            .add_service(intercepted)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await;
        if let Err(e) = served {
            warn!(error = %e, "Flight server stopped with an error");
        }
    });
    info!(%addr, "Flight SQL entrance listening");
    Ok(RunningFlight { addr, stop, task })
}
