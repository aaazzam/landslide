//! SQLite WAL parsing: committed transactions as page post-images.
//!
//! A WAL is a 32-byte header followed by fixed-size frames of
//! `24-byte header + page_size bytes`. A transaction is a run of frames
//! ending at a commit frame (one whose `db_size` field is non-zero); frames
//! after the last commit frame are an uncommitted tail SQLite may overwrite,
//! so they are never consumed. Frame salts must match the header to be valid.
//! v1 trusts salts, not checksums.

pub const HEADER_LEN: usize = 32;
pub const FRAME_HEADER_LEN: usize = 24;

const MAGIC: [u32; 2] = [0x377f0682, 0x377f0683];

/// Read position within one WAL generation. Salts identify the generation:
/// after a checkpoint-truncate the WAL restarts with new salts, and any
/// cursor from the old generation is obsolete (it was fully captured first).
#[derive(Debug, Clone, Copy, Default)]
pub struct WalCursor {
    pub salt1: u32,
    pub salt2: u32,
    /// Next frame index to read (0-based within this generation). Points at
    /// the first frame after the last commit frame.
    pub frame: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub page_size: u32,
    pub salt1: u32,
    pub salt2: u32,
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[..4].try_into().unwrap())
}

/// Parses the 32-byte WAL header; `None` if the file is short or not a WAL
/// (e.g. before the first write, when the file is absent or empty).
pub fn header(wal: &[u8]) -> Option<Header> {
    if wal.len() < HEADER_LEN {
        return None;
    }
    if !MAGIC.contains(&be32(&wal[0..])) {
        return None;
    }
    // Page size 1 is SQLite's on-disk encoding of 65536.
    let page_size = match be32(&wal[8..]) {
        1 => 65536,
        n => n,
    };
    Some(Header { page_size, salt1: be32(&wal[16..]), salt2: be32(&wal[20..]) })
}

/// Frame `index` (0-based) as `(pgno, db_size, data)`; `db_size != 0` marks
/// a commit frame and gives the db size in pages after the transaction.
/// `None` past the end of the file or at a frame whose salts don't match the
/// header (stale tail from a prior generation).
pub fn frame<'a>(wal: &'a [u8], h: &Header, index: u64) -> Option<(u32, u32, &'a [u8])> {
    let frame_len = FRAME_HEADER_LEN + h.page_size as usize;
    let off = HEADER_LEN + index as usize * frame_len;
    let hdr = wal.get(off..off + FRAME_HEADER_LEN)?;
    if be32(&hdr[8..]) != h.salt1 || be32(&hdr[12..]) != h.salt2 {
        return None;
    }
    let data = wal.get(off + FRAME_HEADER_LEN..off + frame_len)?;
    Some((be32(&hdr[0..]), be32(&hdr[4..]), data))
}
