//! The Beekeeper's commands: the Apiary key, join tokens and revocation.
//!
//! The Apiary has one signing key, held by the Beekeeper and kept off the Nodes.
//! It signs the join tokens Nodes present to each other and the list of what has
//! been revoked. Nodes hold only the public half.

use std::path::Path;

use apiary_net::{
    ApiaryKey, Caps, NodeId, PeerHint, Revocations, Token, TokenSpec, Trust, parse_public,
};

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// `apiary key generate`: make the Apiary's signing key.
pub fn key_generate(out: &Path) -> Result<(), String> {
    let key = ApiaryKey::generate();
    key.save_new(out).map_err(|e| e.to_string())?;
    println!(
        "Wrote the Apiary key to {}. Keep it safe and off the Nodes.",
        out.display()
    );
    println!("The public key every Node trusts (put it in [net] apiary_public_key):");
    println!("{}", key.public());
    Ok(())
}

/// `apiary key public`: print the public half of an Apiary key.
pub fn key_public(key: &Path) -> Result<(), String> {
    let key = ApiaryKey::load(key).map_err(|e| e.to_string())?;
    println!("{}", key.public());
    Ok(())
}

/// What `apiary token issue` was asked for.
pub struct IssueArgs<'a> {
    pub key: &'a Path,
    pub apiary: &'a str,
    pub colony: &'a str,
    pub caps: &'a str,
    pub days: i64,
    pub node: Option<&'a str>,
    pub bootstrap: &'a [String],
    pub relay: Option<&'a str>,
}

/// `apiary token issue`: sign a join token.
pub fn token_issue(args: &IssueArgs) -> Result<(), String> {
    let key = ApiaryKey::load(args.key).map_err(|e| e.to_string())?;
    let caps = Caps::parse(args.caps)?;
    let node: Option<NodeId> = args
        .node
        .map(parse_public)
        .transpose()
        .map_err(|e| e.to_string())?;
    let bootstrap = args
        .bootstrap
        .iter()
        .map(|b| parse_bootstrap(b))
        .collect::<Result<Vec<_>, _>>()?;
    if args.days <= 0 {
        return Err("--days must be positive".into());
    }
    let token = key.issue(
        &TokenSpec {
            apiary: args.apiary.to_string(),
            colony: args.colony.to_string(),
            caps,
            lifetime_secs: args.days * 86_400,
            node,
            bootstrap,
            relay: args.relay.map(String::from),
        },
        now(),
    );
    println!("{token}");
    Ok(())
}

/// `id@host:port[,host:port]`.
fn parse_bootstrap(text: &str) -> Result<PeerHint, String> {
    let (id, addrs) = text
        .split_once('@')
        .ok_or_else(|| format!("'{text}' is not id@host:port"))?;
    parse_public(id).map_err(|e| e.to_string())?;
    Ok(PeerHint {
        id: id.to_string(),
        addrs: addrs
            .split(',')
            .filter(|a| !a.is_empty())
            .map(String::from)
            .collect(),
    })
}

/// `apiary token inspect`: show a token's claims, and check its signature if the
/// Apiary's public key is given.
pub fn token_inspect(token: &str, public: Option<&str>) -> Result<(), String> {
    let token = Token::parse(token).map_err(|e| e.to_string())?;
    let c = token.claims();
    println!("apiary:    {}", c.apiary);
    println!("colony:    {}", c.colony);
    println!("caps:      {}", c.caps);
    println!("token id:  {}", c.token_id);
    println!("issued at: {}", fmt_time(c.issued_at));
    println!("expires:   {}", fmt_time(c.not_after));
    println!("node:      {}", c.node.as_deref().unwrap_or("any"));
    println!("relay:     {}", c.relay.as_deref().unwrap_or("none"));
    for peer in &c.bootstrap {
        println!("bootstrap: {} {}", peer.id, peer.addrs.join(","));
    }
    if let Some(public) = public {
        let trust = Trust {
            apiary: c.apiary.clone(),
            key: parse_public(public).map_err(|e| e.to_string())?,
        };
        // Check as if presented by the bound node, or by an arbitrary one.
        let peer = match &c.node {
            Some(n) => parse_public(n).map_err(|e| e.to_string())?,
            None => apiary_net::NodeKey::generate().id(),
        };
        match token.verify(&trust, &peer, now(), &Revocations::empty()) {
            Ok(m) => println!(
                "signature: valid{}",
                if m.clock_suspect {
                    " (this clock is behind the token)"
                } else {
                    ""
                }
            ),
            Err(e) => println!("signature: NOT valid for now: {e}"),
        }
    }
    Ok(())
}

fn fmt_time(secs: i64) -> String {
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| secs.to_string())
}

/// `apiary revoke`: add to the cumulative revocation list and sign it. Returns the
/// new list, which the caller can push to a running Node.
pub fn revoke(
    key: &Path,
    list: &Path,
    nodes: &[String],
    token_ids: &[String],
) -> Result<Revocations, String> {
    if nodes.is_empty() && token_ids.is_empty() {
        return Err("Name something to revoke: --node <id> or --token-id <id>".into());
    }
    let key = ApiaryKey::load(key).map_err(|e| e.to_string())?;
    let current: Revocations = match std::fs::read(list) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| format!("{} is not a revocation list: {e}", list.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Revocations::empty(),
        Err(e) => return Err(format!("Cannot read {}: {e}", list.display())),
    };
    let node_ids = nodes
        .iter()
        .map(|n| parse_public(n).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let next = key.revoke(&current, &node_ids, token_ids, now());
    std::fs::write(
        list,
        serde_json::to_vec_pretty(&next).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("Cannot write {}: {e}", list.display()))?;
    println!(
        "Revocation list is now version {} ({} node(s), {} token(s)), saved to {}.",
        next.seq,
        next.nodes.len(),
        next.tokens.len(),
        list.display()
    );
    Ok(next)
}
