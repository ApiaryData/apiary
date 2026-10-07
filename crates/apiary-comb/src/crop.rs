//! The crop: data landed on a Node and not yet deposited into the comb.
//!
//! A forager carries nectar home in her crop. Here, each ingested batch lands
//! first on the ingesting Node's local disk, queryable at once, and moves into
//! the comb on the Node's deposit cadence. The crop is the one place a Node's
//! death can lose data, so it is written carefully.
//!
//! Each Frame has a directory of numbered segments, an append-only log:
//!
//! ```text
//! <crop>/CROP_ID                        this crop's stable identity
//! <crop>/<hive>/<box>/<frame>/
//!     00000000000000000001.arrow        one Arrow IPC file per ingested batch
//!     00000000000000000002.arrow
//!     00000000000000000002.done         marker: segments up to 2 are deposited
//! ```
//!
//! - **Appends are atomic and durable.** A segment is written under a temporary
//!   name, synced, then renamed, so a crash leaves either a whole segment or none.
//! - **Deposits are idempotent.** A deposit of segments up to `n` commits to the
//!   Delta table with an application transaction `(crop id, n)`. After a crash
//!   between that commit and the cleanup, the next run reads `n` back from the
//!   table and releases those segments instead of depositing them twice.
//! - **Sequence numbers are never reused.** Releasing keeps a `.done` marker for
//!   the highest released segment, so the next segment is always numbered above
//!   anything already deposited. The crop id is stable across restarts, so the
//!   transaction identity survives them.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use apiary_core::{ApiaryError, Result};

const SEGMENT_EXT: &str = "arrow";
const DONE_EXT: &str = "done";
const TMP_EXT: &str = "tmp";
const CROP_ID_FILE: &str = "CROP_ID";

/// Names a Frame in the crop.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FrameKey {
    /// The Hive.
    pub hive: String,
    /// The Box.
    pub box_name: String,
    /// The Frame.
    pub frame: String,
}

/// One ingested batch on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    /// The segment's number: later ingests have higher numbers.
    pub seq: u64,
    /// Where the segment is stored.
    pub path: PathBuf,
    /// Its size on disk.
    pub bytes: u64,
}

/// A Node's crop: a directory of Frame logs.
#[derive(Debug)]
pub struct Crop {
    root: PathBuf,
    id: String,
    sync: bool,
    frames: Mutex<HashMap<FrameKey, Arc<FrameCrop>>>,
}

impl Crop {
    /// Open (creating if needed) the crop at `root`. Appends are synced to disk.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .map_err(|e| io_err(format!("Failed to create crop directory {root:?}"), e))?;
        let id = read_or_create_id(&root)?;
        Ok(Self {
            root,
            id,
            sync: true,
            frames: Mutex::new(HashMap::new()),
        })
    }

    /// Choose whether appends are synced to disk (the default). Turning this off
    /// trades durability for speed and is meant for tests and bulk benchmarks.
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// This crop's stable identity.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The Delta application id under which this crop records its deposits.
    pub fn app_id(&self) -> String {
        format!("apiary.crop/{}", self.id)
    }

    /// The log for one Frame, created on first use.
    pub fn frame(&self, hive: &str, box_name: &str, frame: &str) -> Result<Arc<FrameCrop>> {
        let key = FrameKey {
            hive: hive.to_string(),
            box_name: box_name.to_string(),
            frame: frame.to_string(),
        };
        let mut frames = self.frames.lock().expect("crop poisoned");
        if let Some(existing) = frames.get(&key) {
            return Ok(Arc::clone(existing));
        }
        let dir = self
            .root
            .join(encode_component(hive)?)
            .join(encode_component(box_name)?)
            .join(encode_component(frame)?);
        let opened = Arc::new(FrameCrop::open(dir, self.sync)?);
        frames.insert(key, Arc::clone(&opened));
        Ok(opened)
    }

    /// The log for one Frame if it has one, without creating it. Reads use
    /// this, so querying a Frame never leaves directories behind.
    pub fn frame_if_exists(
        &self,
        hive: &str,
        box_name: &str,
        frame: &str,
    ) -> Result<Option<Arc<FrameCrop>>> {
        let dir = self
            .root
            .join(encode_component(hive)?)
            .join(encode_component(box_name)?)
            .join(encode_component(frame)?);
        if !dir.is_dir() {
            return Ok(None);
        }
        self.frame(hive, box_name, frame).map(Some)
    }

    /// Every Frame that has a log on disk, including ones with nothing pending.
    pub fn frames(&self) -> Result<Vec<FrameKey>> {
        let mut found = Vec::new();
        for hive in list_dirs(&self.root)? {
            for box_dir in list_dirs(&hive.1)? {
                for frame in list_dirs(&box_dir.1)? {
                    found.push(FrameKey {
                        hive: decode_component(&hive.0)?,
                        box_name: decode_component(&box_dir.0)?,
                        frame: decode_component(&frame.0)?,
                    });
                }
            }
        }
        found.sort_by(|a, b| {
            (&a.hive, &a.box_name, &a.frame).cmp(&(&b.hive, &b.box_name, &b.frame))
        });
        Ok(found)
    }

    /// Total bytes of segments not yet released, across every Frame.
    pub fn pending_bytes(&self) -> Result<u64> {
        let mut total = 0;
        for key in self.frames()? {
            let frame = self.frame(&key.hive, &key.box_name, &key.frame)?;
            total += frame.pending()?.iter().map(|s| s.bytes).sum::<u64>();
        }
        Ok(total)
    }
}

/// One Frame's append-only log of segments.
#[derive(Debug)]
pub struct FrameCrop {
    dir: PathBuf,
    sync: bool,
    /// The number the next segment will get. Held while a segment is written, so
    /// segments are numbered in the order they become durable.
    next_seq: Mutex<u64>,
}

impl FrameCrop {
    fn open(dir: PathBuf, sync: bool) -> Result<Self> {
        fs::create_dir_all(&dir)
            .map_err(|e| io_err(format!("Failed to create crop directory {dir:?}"), e))?;

        // A crash mid-write leaves a temporary file; it was never a segment.
        let mut highest = 0;
        for entry in read_dir(&dir)? {
            let path = entry.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some(TMP_EXT) => {
                    let _ = fs::remove_file(&path);
                }
                Some(SEGMENT_EXT | DONE_EXT) => {
                    if let Some(seq) = seq_of(&path) {
                        highest = highest.max(seq);
                    }
                }
                _ => {}
            }
        }
        Ok(Self {
            dir,
            sync,
            next_seq: Mutex::new(highest + 1),
        })
    }

    /// Land a batch as a new segment. Returns `None` for an empty batch.
    ///
    /// Blocking: the segment is written and synced before this returns.
    pub fn append(&self, batch: &RecordBatch) -> Result<Option<Segment>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let mut next = self.next_seq.lock().expect("crop poisoned");

        // Written under a name no other writer can share, then given its
        // number. Two writers on one directory (a misconfiguration) never
        // overwrite each other: the one that loses the race for a number takes
        // the next.
        let tmp = self
            .dir
            .join(format!("{}.{TMP_EXT}", uuid::Uuid::new_v4().simple()));
        let written = (|| -> std::io::Result<(u64, PathBuf, u64)> {
            let file = File::create(&tmp)?;
            let mut writer = FileWriter::try_new(BufWriter::new(&file), &batch.schema())
                .map_err(std::io::Error::other)?;
            writer.write(batch).map_err(std::io::Error::other)?;
            writer.finish().map_err(std::io::Error::other)?;
            drop(writer);
            if self.sync {
                file.sync_all()?;
            }
            let bytes = file.metadata()?.len();

            let mut seq = *next;
            loop {
                let path = self.dir.join(format!("{seq:020}.{SEGMENT_EXT}"));
                match fs::hard_link(&tmp, &path) {
                    Ok(()) => {
                        let _ = fs::remove_file(&tmp);
                        break Ok((seq, path, bytes));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => seq += 1,
                    // A file system without hard links: fall back to a rename,
                    // which is atomic but replaces an existing segment.
                    Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                        if path.exists() {
                            seq += 1;
                        } else {
                            fs::rename(&tmp, &path)?;
                            break Ok((seq, path, bytes));
                        }
                    }
                    Err(e) => break Err(e),
                }
            }
        })();

        let result = written.and_then(|(seq, path, bytes)| {
            if self.sync {
                sync_dir(&self.dir)?;
            }
            Ok((seq, path, bytes))
        });

        match result {
            Ok((seq, path, bytes)) => {
                *next = seq + 1;
                Ok(Some(Segment { seq, path, bytes }))
            }
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                Err(io_err(
                    format!("Failed to write a crop segment in {:?}", self.dir),
                    e,
                ))
            }
        }
    }

    /// The segments not yet released, oldest first.
    pub fn pending(&self) -> Result<Vec<Segment>> {
        let mut segments = Vec::new();
        for entry in read_dir(&self.dir)? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some(SEGMENT_EXT) {
                continue;
            }
            let Some(seq) = seq_of(&path) else { continue };
            let bytes = entry.metadata().map(|m| m.len()).unwrap_or(0);
            segments.push(Segment { seq, path, bytes });
        }
        segments.sort_by_key(|s| s.seq);
        Ok(segments)
    }

    /// The highest segment number released so far (0 if none).
    pub fn released(&self) -> Result<u64> {
        let mut highest = 0;
        for entry in read_dir(&self.dir)? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some(DONE_EXT)
                && let Some(seq) = seq_of(&path)
            {
                highest = highest.max(seq);
            }
        }
        Ok(highest)
    }

    /// Read segments into batches.
    ///
    /// A segment that has vanished returns [`ApiaryError::NotFound`]: it was
    /// deposited and released since it was listed.
    pub fn read(&self, segments: &[Segment]) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for segment in segments {
            let file = File::open(&segment.path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    ApiaryError::NotFound {
                        key: segment.path.display().to_string(),
                    }
                } else {
                    io_err(format!("Failed to open crop segment {:?}", segment.path), e)
                }
            })?;
            let reader = FileReader::try_new(BufReader::new(file), None).map_err(|e| {
                ApiaryError::Internal {
                    message: format!("Corrupt crop segment {:?}: {e}", segment.path),
                }
            })?;
            for batch in reader {
                batches.push(batch.map_err(|e| ApiaryError::Internal {
                    message: format!("Corrupt crop segment {:?}: {e}", segment.path),
                })?);
            }
        }
        Ok(batches)
    }

    /// Release every segment up to and including `up_to`: they are in the comb.
    ///
    /// A marker for `up_to` is written first, then the segments are removed, so
    /// a reader that finds a segment missing also finds the marker.
    pub fn release(&self, up_to: u64) -> Result<()> {
        if up_to == 0 {
            return Ok(());
        }
        let mut next = self.next_seq.lock().expect("crop poisoned");

        let marker = self.dir.join(format!("{up_to:020}.{DONE_EXT}"));
        if !marker.exists() {
            let file = File::create(&marker)
                .map_err(|e| io_err(format!("Failed to write crop marker {marker:?}"), e))?;
            if self.sync {
                file.sync_all()
                    .map_err(|e| io_err("Failed to sync crop marker", e))?;
            }
        }

        for entry in read_dir(&self.dir)? {
            let path = entry.path();
            let Some(seq) = seq_of(&path) else { continue };
            let remove = match path.extension().and_then(|e| e.to_str()) {
                Some(SEGMENT_EXT) => seq <= up_to,
                Some(DONE_EXT) => seq < up_to,
                _ => false,
            };
            if remove {
                let _ = fs::remove_file(&path);
            }
        }
        if self.sync {
            sync_dir(&self.dir).map_err(|e| io_err("Failed to sync crop directory", e))?;
        }
        *next = (*next).max(up_to + 1);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn io_err(context: impl Into<String>, e: std::io::Error) -> ApiaryError {
    ApiaryError::storage(context, e)
}

fn read_dir(dir: &Path) -> Result<Vec<fs::DirEntry>> {
    fs::read_dir(dir)
        .map_err(|e| io_err(format!("Failed to list crop directory {dir:?}"), e))?
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| io_err(format!("Failed to list crop directory {dir:?}"), e))
}

/// Subdirectories of `dir`, as (name, path).
fn list_dirs(dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut dirs = Vec::new();
    for entry in read_dir(dir)? {
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
            && let Some(name) = entry.file_name().to_str()
        {
            dirs.push((name.to_string(), entry.path()));
        }
    }
    Ok(dirs)
}

/// The sequence number in a segment or marker file name.
fn seq_of(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.parse().ok()
}

/// Make a rename or file creation in `dir` durable. A no-op where directories
/// cannot be synced (Windows).
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

fn read_or_create_id(root: &Path) -> Result<String> {
    let path = root.join(CROP_ID_FILE);
    let read = || fs::read_to_string(&path).map(|id| id.trim().to_string());
    if let Ok(existing) = read()
        && !existing.is_empty()
    {
        return Ok(existing);
    }

    // Write a candidate under a unique name and link it into place: if several
    // Nodes create the id at once, one link wins and they all read its value.
    let candidate = uuid::Uuid::new_v4().to_string();
    let tmp = root.join(format!("{}.{TMP_EXT}", uuid::Uuid::new_v4().simple()));
    let create = || -> std::io::Result<String> {
        fs::write(&tmp, &candidate)?;
        File::open(&tmp)?.sync_all()?;
        match fs::hard_link(&tmp, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            // No hard links here: rename, accepting a rare lost race.
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => fs::rename(&tmp, &path)?,
            Err(e) => return Err(e),
        }
        let _ = fs::remove_file(&tmp);
        sync_dir(root)?;
        read()
    };
    create().map_err(|e| io_err("Failed to write the crop id", e))
}

/// A directory name for a Hive, Box or Frame name: letters, digits, `_` and
/// `-` are kept and everything else (including `.` and `/`) is percent-encoded,
/// so a name can never escape its directory.
fn encode_component(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(ApiaryError::Config {
            message: "A crop name cannot be empty".into(),
        });
    }
    let mut out = String::with_capacity(name.len());
    for byte in name.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    Ok(out)
}

fn decode_component(encoded: &str) -> Result<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(value) = u8::from_str_radix(hex, 16) {
                out.push(value);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).map_err(|_| ApiaryError::Config {
        message: format!("Crop directory name is not valid: {encoded}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use tempfile::TempDir;

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("n", Arc::new(Int64Array::from(values.to_vec())) as ArrayRef),
            (
                "s",
                Arc::new(StringArray::from_iter_values(
                    values.iter().map(|v| format!("v{v}")),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    fn open(dir: &TempDir) -> Crop {
        Crop::open(dir.path().join("crop")).unwrap()
    }

    #[test]
    fn appended_segments_read_back_in_order() {
        let dir = TempDir::new().unwrap();
        let crop = open(&dir);
        let frame = crop.frame("h", "b", "f").unwrap();

        let a = frame.append(&batch(&[1, 2])).unwrap().unwrap();
        let b = frame.append(&batch(&[3])).unwrap().unwrap();
        assert_eq!((a.seq, b.seq), (1, 2));
        assert!(a.bytes > 0);

        let pending = frame.pending().unwrap();
        assert_eq!(
            pending.iter().map(|s| s.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let batches = frame.read(&pending).unwrap();
        assert_eq!(rows(&batches), 3);
        assert_eq!(batches[0], batch(&[1, 2]));
    }

    #[test]
    fn an_empty_batch_makes_no_segment() {
        let dir = TempDir::new().unwrap();
        let frame = open(&dir).frame("h", "b", "f").unwrap();
        assert!(frame.append(&batch(&[])).unwrap().is_none());
        assert!(frame.pending().unwrap().is_empty());
    }

    #[test]
    fn the_crop_survives_a_restart() {
        let dir = TempDir::new().unwrap();
        let id = {
            let crop = open(&dir);
            let frame = crop.frame("h", "b", "f").unwrap();
            frame.append(&batch(&[1])).unwrap();
            frame.append(&batch(&[2])).unwrap();
            crop.id().to_string()
        };

        let crop = open(&dir);
        assert_eq!(crop.id(), id, "the crop id is stable across restarts");
        let frame = crop.frame("h", "b", "f").unwrap();
        assert_eq!(frame.pending().unwrap().len(), 2);
        // and new segments continue the numbering
        assert_eq!(frame.append(&batch(&[3])).unwrap().unwrap().seq, 3);
    }

    #[test]
    fn a_new_crop_directory_gets_a_new_identity() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        assert_ne!(open(&a).id(), open(&b).id());
        assert!(open(&a).app_id().starts_with("apiary.crop/"));
    }

    #[test]
    fn a_crash_mid_write_leaves_no_segment() {
        let dir = TempDir::new().unwrap();
        let frame_dir = {
            let crop = open(&dir);
            let frame = crop.frame("h", "b", "f").unwrap();
            frame.append(&batch(&[1])).unwrap();
            dir.path().join("crop/h/b/f")
        };
        // A temporary file, as left by a crash before the rename.
        fs::write(
            frame_dir.join("00000000000000000002.tmp"),
            b"half a segment",
        )
        .unwrap();

        let frame = open(&dir).frame("h", "b", "f").unwrap();
        assert_eq!(frame.pending().unwrap().len(), 1);
        assert!(!frame_dir.join("00000000000000000002.tmp").exists());
        assert_eq!(frame.append(&batch(&[2])).unwrap().unwrap().seq, 2);
    }

    #[test]
    fn release_removes_segments_but_never_reuses_their_numbers() {
        let dir = TempDir::new().unwrap();
        let crop = open(&dir);
        let frame = crop.frame("h", "b", "f").unwrap();
        for v in 1..=3 {
            frame.append(&batch(&[v])).unwrap();
        }

        frame.release(2).unwrap();
        let pending = frame.pending().unwrap();
        assert_eq!(pending.iter().map(|s| s.seq).collect::<Vec<_>>(), vec![3]);
        assert_eq!(frame.released().unwrap(), 2);

        // Release everything: the directory has no segments, but a marker
        // remembers how far numbering got, across a restart too.
        frame.release(3).unwrap();
        assert!(frame.pending().unwrap().is_empty());
        drop(crop);
        let frame = open(&dir).frame("h", "b", "f").unwrap();
        assert_eq!(frame.released().unwrap(), 3);
        assert_eq!(frame.append(&batch(&[4])).unwrap().unwrap().seq, 4);
    }

    #[test]
    fn releasing_beyond_the_local_log_still_raises_the_numbering() {
        // Recovery: the table says segments up to 9 were deposited, but this
        // directory has been recreated and knows nothing of them.
        let dir = TempDir::new().unwrap();
        let frame = open(&dir).frame("h", "b", "f").unwrap();
        frame.release(9).unwrap();
        assert_eq!(frame.append(&batch(&[1])).unwrap().unwrap().seq, 10);
    }

    #[test]
    fn reading_a_released_segment_reports_not_found() {
        let dir = TempDir::new().unwrap();
        let frame = open(&dir).frame("h", "b", "f").unwrap();
        frame.append(&batch(&[1])).unwrap();
        let listed = frame.pending().unwrap();
        frame.release(1).unwrap();
        let err = frame.read(&listed).unwrap_err();
        assert!(matches!(err, ApiaryError::NotFound { .. }), "{err:?}");
    }

    #[test]
    fn concurrent_appends_get_distinct_ordered_numbers() {
        let dir = TempDir::new().unwrap();
        let crop = Arc::new(open(&dir).with_sync(false));
        let frame = crop.frame("h", "b", "f").unwrap();

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let frame = Arc::clone(&frame);
                std::thread::spawn(move || {
                    (0..10)
                        .map(|i| frame.append(&batch(&[t * 100 + i])).unwrap().unwrap().seq)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        assert_eq!(all, (1..=80).collect::<Vec<_>>());
        assert_eq!(rows(&frame.read(&frame.pending().unwrap()).unwrap()), 80);
    }

    #[test]
    fn frames_are_listed_with_their_original_names() {
        let dir = TempDir::new().unwrap();
        let crop = open(&dir);
        crop.frame("farm", "field", "sensors").unwrap();
        crop.frame("My Hive", "a/b", "..").unwrap();

        let keys = crop.frames().unwrap();
        assert!(keys.contains(&FrameKey {
            hive: "farm".into(),
            box_name: "field".into(),
            frame: "sensors".into()
        }));
        assert!(keys.contains(&FrameKey {
            hive: "My Hive".into(),
            box_name: "a/b".into(),
            frame: "..".into()
        }));
    }

    #[test]
    fn names_cannot_escape_the_crop_directory() {
        let dir = TempDir::new().unwrap();
        let crop = open(&dir);
        let frame = crop.frame("..", "..", "..").unwrap();
        frame.append(&batch(&[1])).unwrap();
        // Nothing was written outside the crop directory.
        let outside: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(outside, vec!["crop".to_string()]);
        assert!(crop.frame("", "b", "f").is_err());
    }

    #[test]
    fn pending_bytes_counts_unreleased_segments() {
        let dir = TempDir::new().unwrap();
        let crop = open(&dir);
        assert_eq!(crop.pending_bytes().unwrap(), 0);
        let frame = crop.frame("h", "b", "f").unwrap();
        frame.append(&batch(&[1, 2, 3])).unwrap();
        assert!(crop.pending_bytes().unwrap() > 0);
        frame.release(1).unwrap();
        assert_eq!(crop.pending_bytes().unwrap(), 0);
    }

    #[test]
    fn two_writers_on_one_directory_never_overwrite_each_other() {
        // A misconfiguration (two Nodes, one crop directory) must not lose data.
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("crop");
        let a = Arc::new(Crop::open(&root).unwrap().with_sync(false));
        let b = Arc::new(Crop::open(&root).unwrap().with_sync(false));

        let writers: Vec<_> = [a, b]
            .into_iter()
            .enumerate()
            .map(|(w, crop)| {
                std::thread::spawn(move || {
                    let frame = crop.frame("h", "b", "f").unwrap();
                    for i in 0..50 {
                        frame.append(&batch(&[(w * 100 + i) as i64])).unwrap();
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }

        let reader = Crop::open(&root).unwrap();
        let frame = reader.frame("h", "b", "f").unwrap();
        let pending = frame.pending().unwrap();
        assert_eq!(pending.len(), 100, "every segment survived");
        let mut values: Vec<i64> = frame
            .read(&pending)
            .unwrap()
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        values.sort_unstable();
        let mut expected: Vec<i64> = (0..50).chain(100..150).collect();
        expected.sort_unstable();
        assert_eq!(values, expected);
    }

    #[test]
    fn nodes_creating_the_crop_at_once_agree_on_its_id() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("crop");
        let ids: Vec<String> = (0..8)
            .map(|_| {
                let root = root.clone();
                std::thread::spawn(move || Crop::open(&root).unwrap().id().to_string())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        assert!(ids.windows(2).all(|w| w[0] == w[1]), "{ids:?}");
        assert_eq!(Crop::open(&root).unwrap().id(), ids[0]);
    }

    #[test]
    fn looking_up_a_missing_frame_creates_nothing() {
        let dir = TempDir::new().unwrap();
        let crop = open(&dir);
        assert!(crop.frame_if_exists("h", "b", "f").unwrap().is_none());
        assert!(crop.frames().unwrap().is_empty());

        crop.frame("h", "b", "f").unwrap();
        assert!(crop.frame_if_exists("h", "b", "f").unwrap().is_some());
    }

    #[test]
    fn name_encoding_round_trips() {
        for name in [
            "plain",
            "with space",
            "a.b",
            "..",
            "a/b\\c",
            "ünïcode",
            "100%",
        ] {
            assert_eq!(
                decode_component(&encode_component(name).unwrap()).unwrap(),
                name
            );
        }
    }
}
