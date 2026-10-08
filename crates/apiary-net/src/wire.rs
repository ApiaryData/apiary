//! Framing for the small messages Apiary sends over a stream: a 4-byte length,
//! then JSON. Large payloads (Arrow batches, Cell data) are not framed this way.

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::NetError;

/// The largest framed message accepted.
pub const MAX_FRAME: usize = 8 * 1024 * 1024;

/// Write one framed message.
pub async fn write_frame<W, T>(w: &mut W, message: &T) -> Result<(), NetError>
where
    W: AsyncWrite + Unpin + ?Sized,
    T: Serialize,
{
    let bytes = serde_json::to_vec(message).map_err(|e| NetError::Protocol(e.to_string()))?;
    if bytes.len() > MAX_FRAME {
        return Err(NetError::Protocol(format!(
            "a message of {} bytes is over the {MAX_FRAME} byte limit",
            bytes.len()
        )));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Read one framed message.
pub async fn read_frame<R, T>(r: &mut R) -> Result<T, NetError>
where
    R: AsyncRead + Unpin + ?Sized,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await.map_err(eof_is_closed)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(NetError::Protocol(format!(
            "a message of {len} bytes is over the {MAX_FRAME} byte limit"
        )));
    }
    let mut bytes = vec![0u8; len];
    r.read_exact(&mut bytes).await.map_err(eof_is_closed)?;
    serde_json::from_slice(&bytes)
        .map_err(|e| NetError::Protocol(format!("unreadable message: {e}")))
}

fn eof_is_closed(e: std::io::Error) -> NetError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        NetError::Closed
    } else {
        NetError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip_and_reject_oversize() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_frame(&mut a, &vec![1, 2, 3]).await.unwrap();
        let got: Vec<i32> = read_frame(&mut b).await.unwrap();
        assert_eq!(got, vec![1, 2, 3]);

        // A length prefix over the limit is refused before any allocation.
        a.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        let err = read_frame::<_, Vec<i32>>(&mut b).await.unwrap_err();
        assert!(matches!(err, NetError::Protocol(_)));

        drop(a);
        assert!(matches!(
            read_frame::<_, Vec<i32>>(&mut b).await,
            Err(NetError::Closed)
        ));
    }
}
