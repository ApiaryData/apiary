//! `apiary sql`: run one query against a Node's Flight SQL entrance.

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::Channel;

/// Run `query` against the Flight SQL server at `url` and print the result.
pub async fn sql(url: &str, token: Option<&str>, query: &str) -> Result<(), String> {
    let channel = Channel::from_shared(url.to_string())
        .map_err(|e| format!("'{url}' is not a valid address: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("Cannot connect to {url}: {e}"))?;
    let mut client = FlightSqlServiceClient::new(channel);
    if let Some(token) = token {
        client.set_token(token.to_string());
    }

    let info = client
        .execute(query.to_string(), None)
        .await
        .map_err(|e| format!("{e}"))?;
    let schema = info
        .clone()
        .try_decode_schema()
        .map_err(|e| e.to_string())?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    for endpoint in info.endpoint {
        let Some(ticket) = endpoint.ticket else {
            continue;
        };
        let stream = client.do_get(ticket).await.map_err(|e| e.to_string())?;
        let mut got: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| e.to_string())?;
        batches.append(&mut got);
    }

    print_batches(&batches).map_err(|e| e.to_string())?;
    if batches.is_empty() {
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        println!("(no rows; columns: {})", names.join(", "));
    }
    let meta = schema.metadata();
    if let (Some(crop), Some(comb)) = (meta.get("apiary.rows.crop"), meta.get("apiary.rows.comb")) {
        println!("rows read: {crop} from the crop (not yet shipped), {comb} from the comb");
    }
    Ok(())
}
