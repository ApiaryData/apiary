//! Commands that talk to a running Node's network, and the standalone relay.

use arrow_flight::Action;
use arrow_flight::flight_service_client::FlightServiceClient;
use tonic::transport::Channel;

use apiary_net::{RelayServer, Revocations};

async fn client(url: &str) -> Result<FlightServiceClient<Channel>, String> {
    let channel = Channel::from_shared(url.to_string())
        .map_err(|e| format!("'{url}' is not a valid address: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("Cannot connect to {url}: {e}"))?;
    Ok(FlightServiceClient::new(channel))
}

/// Run a Flight action on a Node and return its JSON reply.
async fn action(
    url: &str,
    token: Option<&str>,
    kind: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let mut client = client(url).await?;
    let mut request = tonic::Request::new(Action {
        r#type: kind.to_string(),
        body: serde_json::to_vec(&body).map_err(|e| e.to_string())?.into(),
    });
    if let Some(token) = token {
        let value = format!("Bearer {token}")
            .parse()
            .map_err(|_| "The token has characters a header cannot carry".to_string())?;
        request.metadata_mut().insert("authorization", value);
    }
    let mut stream = client
        .do_action(request)
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    let first = stream
        .message()
        .await
        .map_err(|e| e.message().to_string())?
        .ok_or("The node sent no reply")?;
    serde_json::from_slice(&first.body).map_err(|e| e.to_string())
}

/// `apiary net status`: print what a Node sees of its network.
pub async fn status(url: &str, token: Option<&str>, json: bool) -> Result<(), String> {
    let status = action(url, token, "apiary.net_status", serde_json::json!({})).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&status).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    let text = |v: &serde_json::Value| v.as_str().unwrap_or("-").to_string();
    println!("node      {}", text(&status["node_id"]));
    println!(
        "apiary    {} / colony {} / site {}",
        text(&status["apiary"]),
        text(&status["colony"]),
        text(&status["site"])
    );
    println!(
        "serves    {}",
        if status["serves_comb"].as_bool() == Some(true) {
            "the comb (this node is the comb host)"
        } else {
            "-"
        }
    );
    println!("relay     {}", text(&status["relay"]));
    println!("revoked   list version {}", status["revocations_seq"]);
    let peers = status["peers"].as_array().cloned().unwrap_or_default();
    println!("\npeers ({}):", peers.len());
    for p in &peers {
        println!(
            "  {}  {:<9} {:>8}  {:<10} colony {}  caps {}{}",
            &text(&p["id"])[..text(&p["id"]).len().min(12)],
            text(&p["path"]),
            p["rtt_ms"]
                .as_f64()
                .map_or("-".to_string(), |r| format!("{r:.1} ms")),
            text(&p["verdict"]).split(':').next().unwrap_or(""),
            text(&p["colony"]),
            text(&p["caps"]),
            if p["clock_suspect"].as_bool() == Some(true) {
                "  (clock behind)"
            } else {
                ""
            },
        );
    }
    let found = status["discovered"].as_array().cloned().unwrap_or_default();
    let waiting: Vec<_> = found
        .iter()
        .filter(|d| d["connected"].as_bool() != Some(true))
        .collect();
    if !waiting.is_empty() {
        println!("\nfound but not joined ({}):", waiting.len());
        for d in waiting {
            println!(
                "  {}  via {}  {}",
                &text(&d["id"])[..text(&d["id"]).len().min(12)],
                d["sources"],
                text(&d["last_error"])
            );
        }
    }
    Ok(())
}

/// Hand a signed revocation list to a running Node, which passes it on.
pub async fn push_revocations(
    url: &str,
    token: Option<&str>,
    list: &Revocations,
) -> Result<(), String> {
    let body = serde_json::to_value(list).map_err(|e| e.to_string())?;
    let reply = action(url, token, "apiary.net_revoke", body).await?;
    println!(
        "{} {}",
        url,
        if reply["adopted"].as_bool() == Some(true) {
            "adopted the list and is passing it on."
        } else {
            "already had this version or a newer one."
        }
    );
    Ok(())
}

/// `apiary relay run`: a relay with no Node attached, for a cloud VM.
pub async fn relay(listen: &str) -> Result<(), String> {
    let addr = listen
        .parse()
        .map_err(|e| format!("'{listen}' is not an address: {e}"))?;
    let server = RelayServer::spawn(addr).await.map_err(|e| e.to_string())?;
    println!(
        "Relay serving plain HTTP on {}. Nodes use it as http://<this host>:{}; put TLS in front for the open internet.",
        server.addr().map_or(listen.to_string(), |a| a.to_string()),
        server.addr().map_or(0, |a| a.port())
    );
    tokio::signal::ctrl_c().await.map_err(|e| e.to_string())?;
    server.shutdown().await;
    Ok(())
}
