use std::{
    cmp::Ordering,
    fmt,
    fs::{self, File, OpenOptions},
    io, mem,
    path::{Path, PathBuf},
    u64, usize,
};

use memmap2::MmapMut;
use tracing::{debug, info, trace, warn};

use crate::{
    message::{MessageError, MessageKind, MessageSet, HEADER_SIZE},
    reader::MessageBufReader,
    segment::Segment,
    to_page_size,
};

use super::Offset;

/// Number of bytes in each entry pair
pub const INDEX_ENTRY_BYTES: usize = 8;
/// Number of bytes contained in the base name of the file.
pub const INDEX_FILE_NAME_LEN: usize = 20;
/// File extension for the index file.
pub static INDEX_FILE_NAME_EXTENSION: &str = "index";

#[inline]
fn binary_search<F>(index: &[u8], f: F) -> usize
where
    F: Fn(u32, u32) -> Ordering,
{
    assert_eq!(index.len() % INDEX_ENTRY_BYTES, 0);

    let mut i = 0usize;
    let mut j = (index.len() / INDEX_ENTRY_BYTES).saturating_sub(1);

    while i < j {
        // grab midpoint
        let m = i + ((j - i) / 2);

        // read the relative offset at the midpoint
        let mi = m * INDEX_ENTRY_BYTES;
        let rel_off = u32::from_le_bytes(index[mi..mi + 4].try_into().unwrap());
        let file_pos = u32::from_le_bytes(index[mi + 4..mi + 8].try_into().unwrap());

        match f(rel_off, file_pos) {
            Ordering::Equal => return m,
            Ordering::Less => {
                i = m + 1;
            }
            Ordering::Greater => {
                j = m;
            }
        }
    }
    i
}

macro_rules! entry {
    ($mem:ident, $pos:expr) => {
        (
            u32::from_le_bytes($mem[($pos)..($pos) + 4].try_into().unwrap()),
            u32::from_le_bytes($mem[($pos) + 4..($pos) + 8].try_into().unwrap()),
        )
    };
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum RangeFindError {
    /// The starting offset supplied was not found.
    OffsetNotAppended,
    /// The offset requested exceeded the max bytes.
    MessageExceededMaxBytes,
}

impl fmt::Display for RangeFindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeFindError::OffsetNotAppended => write!(f, "offset not appended"),
            RangeFindError::MessageExceededMaxBytes => write!(f, "message exceeded max bytes"),
        }
    }
}

/// Range within a single segment file of messages.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct MessageSetRange {
    file_pos: u32,
    bytes: u32,
}

impl MessageSetRange {
    pub fn file_position(self) -> u32 {
        self.file_pos
    }

    pub fn bytes(self) -> u32 {
        self.bytes
    }
}

/// An index is a file with pairs of relative offset to file position offset
/// of messages at the relative offset messages. The index is Memory Mapped.
pub struct Index {
    file: File,
    path: PathBuf,
    mmap: MmapMut,
    mode: AccessMode,

    /// next starting byte in index file offset to write
    next_write_pos: usize,
    last_flush_end_pos: usize,
    base_offset: u64,
}

/// Describes the access mode of the index
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessMode {
    /// Only reads are permitted.
    Read,
    /// This is the active index and can be read or written to.
    ReadWrite,
}

/// Buffer used to amortize writes to the index.
pub struct IndexBuf(Vec<u8>, u64);

impl IndexBuf {
    pub fn new(len: usize, starting_offset: u64) -> IndexBuf {
        IndexBuf(Vec::with_capacity(len * INDEX_ENTRY_BYTES), starting_offset)
    }

    pub fn push(&mut self, abs_offset: u64, position: u32) {
        // TODO: assert that the offset is > previous
        assert!(
            abs_offset >= self.1,
            "Attempt to append to an offset before base offset in index"
        );

        let mut tmp_buf = [0u8; INDEX_ENTRY_BYTES];
        tmp_buf[0..4].copy_from_slice(&((abs_offset - self.1) as u32).to_le_bytes());
        tmp_buf[4..].copy_from_slice(&position.to_le_bytes());
        self.0.extend_from_slice(&tmp_buf);
    }

    pub fn drain(&mut self) -> IndexBuf {
        IndexBuf(mem::take(&mut self.0), self.1)
    }
}

impl Index {
    pub fn new<P>(log_dir: P, base_offset: u64, file_bytes: usize) -> io::Result<Index>
    where
        P: AsRef<Path>,
    {
        // open the file, expecting to create it
        let path = {
            let mut path_buf = PathBuf::new();
            path_buf.push(&log_dir);
            path_buf.push(format!("{:020}", base_offset));
            path_buf.set_extension(INDEX_FILE_NAME_EXTENSION);
            path_buf
        };

        info!("Creating index file {:?}", &path);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create_new(true)
            .open(&path)?;

        // read the metadata and truncate
        let meta = file.metadata()?;
        let len = meta.len();
        if len == 0 {
            file.set_len(file_bytes as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Index {
            file,
            path,
            mmap,
            mode: AccessMode::ReadWrite,
            next_write_pos: 0,
            last_flush_end_pos: 0,
            base_offset,
        })
    }

    pub fn open<P>(path: P) -> io::Result<Index>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&path)?;

        let filename = path.as_ref().file_name().unwrap().to_str().unwrap();
        let base_offset = match (&filename[0..INDEX_FILE_NAME_LEN]).parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Index file name does not parse as u64",
                ));
            }
        };

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let (next_write_pos, mode) = {
            let index = &mmap[..];
            assert_eq!(index.len() % INDEX_ENTRY_BYTES, 0);

            // check if this is a full or partial index
            match entry!(index, index.len() - INDEX_ENTRY_BYTES) {
                (0, 0) => {
                    // partial index, search for break point
                    let write_pos = INDEX_ENTRY_BYTES
                        * binary_search(index, |rel_off, file_off| {
                            // find the first unwritten index entry:
                            // +#############-----|----------------+
                            //  written msgs            empty msgs
                            // if the file pos is 0, we're in the empty msgs, go left
                            // otherwise, we're in the written msgs, go right
                            //
                            // NOTE: it is assumed the segment will never start at 0
                            // since it contains at least 1 magic byte
                            if file_off == 0 && rel_off == 0 {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        });
                    (write_pos, AccessMode::ReadWrite)
                }
                _ => (index.len(), AccessMode::Read),
            }
        };

        debug!(
            %filename, %next_write_pos, ?mode,
            "opening index",
        );

        Ok(Index {
            file,
            path: path.as_ref().to_path_buf(),
            mmap,
            mode,
            next_write_pos,
            last_flush_end_pos: next_write_pos,
            base_offset,
        })
    }

    pub fn rehydrate(&mut self, segment: &Segment, message_max_bytes: usize) -> io::Result<()> {
        assert_eq!(
            self.mode,
            AccessMode::ReadWrite,
            "Attempt to rehydrate to readonly index"
        );

        let latest = self.next_offset();
        let mut pos = self.last().unwrap_or(2);
        let mut reader = MessageBufReader;

        let mut current_tx = None;
        let mut buf = IndexBuf::new(0, segment.starting_offset());

        loop {
            let msgs = segment
                .read_slice_partial(&mut reader, pos, (message_max_bytes + HEADER_SIZE) as u32)
                .map_err(|err| match err {
                    MessageError::IoError(err) => err,
                    MessageError::InvalidHash => io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid hash at position {pos}"),
                    ),
                    MessageError::InvalidPayloadLength => {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid payload length")
                    }
                    MessageError::InvalidMessageKind => {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid message kind")
                    }
                })?;

            if msgs.is_empty() {
                break;
            }

            for msg in msgs.iter() {
                match msg.kind() {
                    MessageKind::Event => {
                        if msg.offset() < latest {
                            // This event is already indexed, skip it
                            pos += msg.total_bytes() as u32; // Move the position along
                            continue;
                        }

                        let tx_id = msg.tx();
                        if current_tx.is_none() {
                            // Start tracking a new transaction
                            current_tx = Some(tx_id);
                        }
                        if current_tx == Some(tx_id) {
                            // Buffer the message to be indexed if the transaction is committed later
                            buf.push(msg.offset(), pos);
                        }
                    }
                    MessageKind::Commit => {
                        let tx_id = msg.tx();
                        if current_tx == Some(tx_id) {
                            // Commit the transaction, meaning all buffered events are now valid
                            self.append(buf.drain())?;
                            current_tx = None; // Reset the current transaction
                        }
                    }
                }

                pos += msg.total_bytes() as u32;
            }
        }

        self.flush().unwrap();

        Ok(())
    }

    #[inline]
    pub fn starting_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn is_empty(&self) -> bool {
        self.next_write_pos == 0
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.mmap.len()
    }

    #[inline]
    pub fn last(&self) -> Option<u32> {
        self.find(self.next_offset().checked_sub(1)?)
            .map(|(_, file_pos)| file_pos)
    }

    // TODO: use memremap on linux
    fn resize(&mut self) -> io::Result<()> {
        // increase length by 50% -= 7 for alignment
        let new_len = {
            let l = self.size();
            let new_size = l + (l / 2);
            // align to byte size
            new_size - (new_size % INDEX_ENTRY_BYTES)
        };

        // unmap the file (Set to dummy anonymous map)
        self.mmap = MmapMut::map_anon(32)?;
        self.file.set_len(new_len as u64)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    pub fn append(&mut self, offsets: IndexBuf) -> io::Result<()> {
        // TODO: trace
        //trace!("Index append: {:?}", abs_offset, position);

        assert_eq!(
            self.base_offset, offsets.1,
            "Buffer starting offset does not match the index starting offset"
        );
        assert_eq!(
            self.mode,
            AccessMode::ReadWrite,
            "Attempt to append to readonly index"
        );

        // check if we need to resize
        if self.size() < (self.next_write_pos + offsets.0.len()) {
            self.resize()?;
        }

        let start = self.next_write_pos;
        let end = start + offsets.0.len();

        self.mmap[start..end].copy_from_slice(&offsets.0);

        self.next_write_pos = end;

        Ok(())
    }

    pub fn set_readonly(&mut self) -> io::Result<()> {
        if self.mode != AccessMode::Read {
            self.mode = AccessMode::Read;

            // trim un-used entries by reducing mmap view and truncating file
            if self.next_write_pos < self.mmap.len() {
                // TODO: fix restrict
                // self.mmap.restrict(0, self.next_write_pos)?;
                if let Err(err) = self.file.set_len(self.next_write_pos as u64) {
                    warn!(
                        "Unable to truncate index file {:020}.{} to proper length: {:?}",
                        self.base_offset, INDEX_FILE_NAME_EXTENSION, err
                    );
                }
            }

            self.flush()
        } else {
            Ok(())
        }
    }

    pub fn remove(self) -> io::Result<()> {
        let path = self.path.clone();
        drop(self);

        info!("Removing index file {}", path.display());
        fs::remove_file(path)
    }

    /// Truncates to an offset, inclusive. The file length of the
    /// segment for truncation is returned.
    pub fn truncate(&mut self, offset: Offset) -> Option<u32> {
        // find the next offset position in order to inform
        // the truncation of the segment
        let next_pos = match self.find_index_pos(offset + 1) {
            Some(i) => {
                trace!("Found offset mem offset {}", i);
                i
            }
            None => {
                trace!("No offset {} found in index", offset + 1);
                return None;
            }
        };

        let mem = &mut self.mmap[..];

        let (off, file_len) = entry!(mem, next_pos);

        // find_index_pos will find the right-most position, which may include
        // something <= the offset passed in, which we should reject for
        // truncation. This likely occurs when the last offset is the offset
        // requested for truncation OR the offset for truncation is > than the
        // last offset.
        if u64::from(off) + self.base_offset <= offset {
            trace!("Truncated to exact segment boundary, no need to truncate segment");
            return None;
        }

        trace!(
            "Start of truncation at offset {}, to segment length {}",
            offset,
            file_len
        );

        // override file positions > offset
        for elem in &mut mem[next_pos..self.next_write_pos].iter_mut() {
            *elem = 0;
        }

        // re-adjust the next file pos
        self.next_write_pos = next_pos;

        Some(file_len)
    }

    /// Flush the index at page boundaries. This may leave some indexed values
    /// not flushed during crash, which will be rehydrated on restart.
    pub fn flush(&mut self) -> io::Result<()> {
        let start = to_page_size(self.last_flush_end_pos);
        let end = to_page_size(self.next_write_pos);

        if end > start {
            self.mmap.flush_range(start, end - start)?;
            self.last_flush_end_pos = end;
        }

        Ok(())
    }

    pub fn next_offset(&self) -> Offset {
        if self.next_write_pos == 0 {
            self.base_offset
        } else {
            let entry = self
                .read_entry((self.next_write_pos / INDEX_ENTRY_BYTES) - 1)
                .unwrap();
            entry.0 + 1
        }
    }

    pub fn read_entry(&self, i: usize) -> Option<(Offset, u32)> {
        if self.size() < (i + 1) * 8 {
            return None;
        }

        let mem_slice = &self.mmap[..];
        let start = i * INDEX_ENTRY_BYTES;
        let offset = u32::from_le_bytes(mem_slice[start..start + 4].try_into().unwrap());
        if offset == 0 && i > 0 {
            None
        } else {
            let pos = u32::from_le_bytes(mem_slice[start + 4..start + 8].try_into().unwrap());
            Some((u64::from(offset) + self.base_offset, pos))
        }
    }

    /// Finds the index entry corresponding to the offset.
    ///
    /// If the entry does not exist in the index but an entry > the offset
    /// exists, that entry is used.
    ///
    /// If the entry does not exist and the last entry is < the desired,
    /// the offset has not been written to this index and None value is
    /// returned.
    #[allow(dead_code)]
    pub fn find(&self, offset: Offset) -> Option<(Offset, u32)> {
        self.find_index_pos(offset).and_then(|p| {
            let mem_slice = &self.mmap[..];
            let (rel_off, file_pos) = entry!(mem_slice, p);
            let abs_off = u64::from(rel_off) + self.base_offset;
            if abs_off < offset {
                None
            } else {
                Some((abs_off, file_pos))
            }
        })
    }

    /// Finds the longest message set range within a single segment aligning to
    /// the `max_bytes` parameter.
    pub fn find_segment_range(
        &self,
        offset: Offset,
        max_bytes: u32,
        seg_bytes: u32,
    ) -> Result<MessageSetRange, RangeFindError> {
        assert!(max_bytes > 0, "Cannot request 0 bytes to be read");

        // position within the index to start finding a sequence
        let start_ind_pos = match self.find_index_pos(offset) {
            Some(v) => v,
            _ => return Err(RangeFindError::OffsetNotAppended),
        };

        let mem_slice = &self.mmap[..];
        let (_, start_file_pos) = entry!(mem_slice, start_ind_pos);

        // try to get until the end of the segment
        if seg_bytes - start_file_pos < max_bytes {
            trace!("Requested range contains the rest of the segment, does not exceed max bytes");
            return Ok(MessageSetRange {
                file_pos: start_file_pos,
                bytes: seg_bytes - start_file_pos,
            });
        }

        let search_range = &mem_slice[start_ind_pos..self.next_write_pos];
        if search_range.is_empty() {
            return Err(RangeFindError::MessageExceededMaxBytes);
        }

        let end_ind_pos = binary_search(search_range, |_, pos| {
            (pos - start_file_pos).cmp(&max_bytes)
        });

        let pos = {
            // binary search will choose the next entry when the left value is less, and the
            // right value is greater and not equal, so fix by grabbing the left
            let (_, pos) = entry!(search_range, end_ind_pos * INDEX_ENTRY_BYTES);
            if end_ind_pos > 0 && pos - start_file_pos > max_bytes {
                trace!("Binary search yielded a range too large, trying entry before");
                let (_, pos) = entry!(search_range, (end_ind_pos - 1) * INDEX_ENTRY_BYTES);
                pos
            } else {
                pos
            }
        };

        let bytes = pos - start_file_pos;
        if bytes == 0 || bytes > max_bytes {
            Err(RangeFindError::MessageExceededMaxBytes)
        } else {
            trace!("Found slice range {}..{}", start_file_pos, pos);
            Ok(MessageSetRange {
                file_pos: start_file_pos,
                bytes,
            })
        }
    }

    pub fn find_index_pos(&self, offset: Offset) -> Option<usize> {
        if offset < self.base_offset {
            // pathological case... not worth exposing Result
            return None;
        }

        let rel_offset = (offset - self.base_offset) as u32;

        let mem_slice = &self.mmap[..];
        trace!("offset={} Next write pos = {}", offset, self.next_write_pos);

        // attempt to find the offset assuming no truncation
        // and fall back to binary search otherwise
        if (rel_offset as usize) < self.next_write_pos / INDEX_ENTRY_BYTES {
            trace!("Attempting to read offset from exact location");
            // read exact entry
            let entry_pos = rel_offset as usize * INDEX_ENTRY_BYTES;
            let rel_offset_val =
                u32::from_le_bytes(mem_slice[entry_pos..entry_pos + 4].try_into().unwrap());
            trace!(
                "Found relative offset. rel_offset = {}, entry offset = {}",
                rel_offset,
                rel_offset_val
            );
            if rel_offset_val == rel_offset {
                return Some(entry_pos);
            }
        }

        let i = binary_search(&mem_slice[0..self.next_write_pos], |v, _| {
            v.cmp(&rel_offset)
        });
        trace!("Found offset {} at entry {}", offset, i);

        if i < self.next_write_pos / INDEX_ENTRY_BYTES {
            Some(i * INDEX_ENTRY_BYTES)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::message::{set_offsets, MessageBuf};

    use super::{super::testutil::*, *};

    use std::{fs, path::PathBuf};

    #[test]
    pub fn index() {
        let path = TestDir::new();
        let mut index = Index::new(&path, 9u64, 1000usize).unwrap();

        assert_eq!(1000, index.size());

        let mut buf = IndexBuf::new(2, 9u64);
        buf.push(11u64, 0xffff);
        buf.push(12u64, 0xeeee);
        index.append(buf).unwrap();
        index.flush().unwrap();

        let e0 = index.read_entry(0).unwrap();
        assert_eq!(11u64, e0.0);
        assert_eq!(0xffff, e0.1);

        let e1 = index.read_entry(1).unwrap();
        assert_eq!(12u64, e1.0);
        assert_eq!(0xeeee, e1.1);

        // read an entry that does not exist
        let e2 = index.read_entry(2);
        assert_eq!(None, e2);
    }

    #[test]
    pub fn index_set_readonly() {
        let path = TestDir::new();
        let mut index = Index::new(&path, 10u64, 1000usize).unwrap();

        let mut buf = IndexBuf::new(2, 10u64);
        buf.push(11u64, 0xffff);
        buf.push(12u64, 0xeeee);
        index.append(buf).unwrap();
        index.flush().unwrap();

        // set_readonly it
        index.set_readonly().expect("Unable to set readonly");

        assert_eq!(AccessMode::Read, index.mode);

        let e1 = index.read_entry(1).unwrap();
        assert_eq!(12u64, e1.0);
        assert_eq!(0xeeee, e1.1);

        // read an entry that does not exist
        let e2 = index.read_entry(2);
        assert_eq!(None, e2);
    }

    #[test]
    pub fn open_index() {
        let dir = TestDir::new();
        // issue some writes
        {
            let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();

            {
                let mut buf = IndexBuf::new(3, 10u64);
                buf.push(10, 0);
                buf.push(11, 10);
                buf.push(12, 20);
                index.append(buf).unwrap();
            }

            {
                let mut buf = IndexBuf::new(2, 10u64);
                buf.push(13, 30);
                buf.push(14, 40);
                index.append(buf).unwrap();
            }

            index.flush().unwrap();
            index.set_readonly().unwrap();
        }

        // now open it
        {
            let mut index_path = PathBuf::new();
            index_path.push(&dir);
            index_path.push("00000000000000000010.index");

            let meta = fs::metadata(&index_path).unwrap();
            assert!(meta.is_file());

            let index = Index::open(&index_path).unwrap();

            for i in 0..5usize {
                let entry = index.read_entry(i);
                assert!(entry.is_some());
                assert_eq!(entry.unwrap().0, (i + 10) as u64);
                assert_eq!(entry.unwrap().1, (i * 10) as u32);
            }
        }
    }

    #[test]
    pub fn open_index_with_one_message() {
        let dir = TestDir::new();
        // issue some writes
        {
            let mut index = Index::new(&dir, 0u64, 1000usize).unwrap();

            {
                let mut buf = IndexBuf::new(1, 0u64);
                buf.push(0, 2);
                index.append(buf).unwrap();
            }

            index.flush().unwrap();
        }

        // now open it
        {
            let mut index_path = PathBuf::new();
            index_path.push(&dir);
            index_path.push("00000000000000000000.index");

            let meta = fs::metadata(&index_path).unwrap();
            assert!(meta.is_file());

            let mut index = Index::open(&index_path).unwrap();

            // Issue a new write, to make sure we're not overwriting things
            {
                let mut buf = IndexBuf::new(1, 0u64);
                buf.push(1, 3);
                index.append(buf).unwrap();
            }

            assert_eq!(index.next_write_pos, 16);

            let entry = index.read_entry(0);
            assert!(entry.is_some());
            assert_eq!(entry.unwrap().0, 0_u64);
            assert_eq!(entry.unwrap().1, 2_u32);
        }
    }

    #[test]
    pub fn open_index_with_one_message_closed() {
        let dir = TestDir::new();
        // issue some writes
        {
            let mut index = Index::new(&dir, 0u64, 1000usize).unwrap();

            {
                let mut buf = IndexBuf::new(1, 0u64);
                buf.push(0, 2);
                index.append(buf).unwrap();
            }

            index.flush().unwrap();
            index.set_readonly().unwrap();
        }

        // now open it
        {
            let mut index_path = PathBuf::new();
            index_path.push(&dir);
            index_path.push("00000000000000000000.index");

            let meta = fs::metadata(&index_path).unwrap();
            assert!(meta.is_file());

            let index = Index::open(&index_path).unwrap();
            assert_eq!(index.next_write_pos, 8);
            assert_eq!(AccessMode::Read, index.mode);

            let entry = index.read_entry(0);
            assert!(entry.is_some());
            assert_eq!(entry.unwrap().0, 0_u64);
            assert_eq!(entry.unwrap().1, 2_u32);
        }
    }

    #[test]
    pub fn find() {
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();
        let mut buf = IndexBuf::new(8, 10u64);
        buf.push(10, 1);
        buf.push(11, 2);
        buf.push(12, 3);
        buf.push(15, 4);
        buf.push(16, 5);
        buf.push(17, 6);
        buf.push(18, 7);
        buf.push(20, 8);
        index.append(buf).unwrap();

        let res = index.find(16).unwrap();
        assert_eq!(16, res.0);
        assert_eq!(5, res.1);
    }

    #[test]
    pub fn find_exact() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();
        let mut buf = IndexBuf::new(8, 10u64);
        buf.push(10, 1);
        buf.push(11, 2);
        buf.push(12, 3);
        buf.push(13, 4);
        buf.push(14, 5);
        buf.push(15, 6);
        buf.push(16, 7);
        buf.push(17, 8);
        index.append(buf).unwrap();

        let res = index.find(16).unwrap();
        assert_eq!(16, res.0);
        assert_eq!(7, res.1);
    }

    #[test]
    pub fn find_nonexistant_value_finds_next() {
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();
        let mut buf = IndexBuf::new(8, 10u64);
        buf.push(10, 1);
        buf.push(11, 2);
        buf.push(12, 3);
        buf.push(15, 4);
        buf.push(16, 5);
        buf.push(17, 6);
        buf.push(18, 7);
        buf.push(20, 8);
        index.append(buf).unwrap();

        let res = index.find(14).unwrap();
        assert_eq!(15, res.0);
        assert_eq!(4, res.1);
    }

    #[test]
    pub fn find_nonexistant_value_greater_than_max() {
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();
        let mut buf = IndexBuf::new(8, 10u64);
        buf.push(10, 1);
        buf.push(11, 2);
        buf.push(12, 3);
        buf.push(15, 4);
        buf.push(16, 5);
        buf.push(17, 6);
        buf.push(18, 7);
        buf.push(20, 8);
        index.append(buf).unwrap();

        let res = index.find(21);
        assert!(res.is_none());
    }

    #[test]
    pub fn find_out_of_bounds() {
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();
        let mut buf = IndexBuf::new(8, 10u64);
        buf.push(10, 1);
        buf.push(11, 2);
        buf.push(12, 3);
        buf.push(15, 4);
        buf.push(16, 5);
        buf.push(17, 6);
        buf.push(18, 7);
        buf.push(20, 8);
        index.append(buf).unwrap();

        let res = index.find(2);
        assert!(res.is_none());
    }

    #[test]
    pub fn reopen_partial_index() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        {
            let mut index = Index::new(&dir, 10u64, 1000usize).unwrap();
            let mut buf = IndexBuf::new(8, 10u64);
            buf.push(10, 1);
            buf.push(11, 2);
            index.append(buf).unwrap();
            index.flush().unwrap();
        }

        {
            let mut index_path = PathBuf::new();
            index_path.push(&dir);
            index_path.push("00000000000000000010.index");
            let index = Index::open(&index_path).unwrap();

            let e0 = index.find(10);
            assert!(e0.is_some());
            assert_eq!(10, e0.unwrap().0);

            let e1 = index.find(11);
            assert!(e1.is_some());
            assert_eq!(11, e1.unwrap().0);

            let e2 = index.find(12);
            assert!(e2.is_none());

            assert_eq!(12, index.next_offset());

            // assert_eq!(16, index.size());
            assert_eq!(AccessMode::ReadWrite, index.mode);
        }
    }

    #[test]
    pub fn reopen_full_index() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        {
            let mut index = Index::new(&dir, 10u64, 16usize).unwrap();
            let mut buf = IndexBuf::new(2, 10u64);
            buf.push(10, 1);
            buf.push(11, 2);
            index.append(buf).unwrap();
            index.flush().unwrap();
        }

        {
            let mut index_path = PathBuf::new();
            index_path.push(&dir);
            index_path.push("00000000000000000010.index");
            let index = Index::open(&index_path).unwrap();

            let e0 = index.find(10);
            assert!(e0.is_some());
            assert_eq!(10, e0.unwrap().0);

            let e1 = index.find(11);
            assert!(e1.is_some());
            assert_eq!(11, e1.unwrap().0);

            let e2 = index.find(12);
            assert!(e2.is_none());

            assert_eq!(12, index.next_offset());
        }
    }

    #[test]
    fn find_segment_range_offset() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 40usize).unwrap();
        // -----
        // INSERTION POINT
        //  => 5 messages, each 10 bytes
        // -----
        let mut buf = IndexBuf::new(5, 10u64);
        buf.push(10, 10);
        buf.push(11, 20);
        buf.push(12, 30);
        buf.push(13, 40);
        buf.push(14, 50);
        index.append(buf).unwrap();

        // test offset not in index
        let res = index.find_segment_range(9, 50, 60);
        assert_eq!(Err(RangeFindError::OffsetNotAppended), res);

        // test message exceeds max bytes
        let res = index.find_segment_range(10, 5, 60);
        assert_eq!(Err(RangeFindError::MessageExceededMaxBytes), res);

        // test message within range, not including last message
        let res = index.find_segment_range(10, 20, 60);
        assert_eq!(
            Ok(MessageSetRange {
                file_pos: 10,
                bytes: 20
            }),
            res
        );

        // test message within range, not including last message, not first
        let res = index.find_segment_range(11, 20, 60);
        assert_eq!(
            Ok(MessageSetRange {
                file_pos: 20,
                bytes: 20
            }),
            res
        );

        // test message within rest of range, not including last message
        let res = index.find_segment_range(11, 80, 60);
        assert_eq!(
            Ok(MessageSetRange {
                file_pos: 20,
                bytes: 40
            }),
            res
        );
    }

    #[test]
    fn index_resize() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 32usize).unwrap();
        assert_eq!(32, index.size());
        let mut buf = IndexBuf::new(4, 10u64);
        buf.push(10, 10);
        buf.push(11, 20);
        buf.push(12, 30);
        buf.push(13, 40);
        index.append(buf).unwrap();
        assert_eq!(32, index.size());

        let mut buf = IndexBuf::new(1, 10u64);
        buf.push(14, 50);
        assert!(index.append(buf).is_ok());

        // make sure the index was resized
        assert_eq!(48, index.size());

        assert_eq!(50, index.find(14).unwrap().1);
    }

    #[test]
    fn index_remove() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let index = Index::new(&dir, 0u64, 32usize).unwrap();

        let ind_exists = fs::read_dir(&dir)
            .unwrap()
            .find(|entry| {
                let path = entry.as_ref().unwrap().path();
                path.file_name().unwrap() == "00000000000000000000.index"
            })
            .is_some();
        assert!(ind_exists, "Index file does not exist?");

        // remove the index
        index.remove().expect("Unable to remove file");

        let ind_exists = fs::read_dir(&dir)
            .unwrap()
            .find(|entry| {
                let path = entry.as_ref().unwrap().path();
                path.file_name().unwrap() == "00000000000000000000.index"
            })
            .is_some();
        assert!(!ind_exists, "Index should not exist");
    }

    #[test]
    fn index_truncate() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 128usize).unwrap();
        let mut buf = IndexBuf::new(5, 10u64);
        buf.push(10, 10);
        buf.push(11, 20);
        buf.push(12, 30);
        buf.push(13, 40);
        buf.push(14, 50);
        index.append(buf).unwrap();

        let file_len = index.truncate(12);
        assert_eq!(Some(40), file_len);
        assert_eq!(13, index.next_offset());
        assert_eq!(3 * INDEX_ENTRY_BYTES, index.next_write_pos);

        // ensure we've zeroed the entries
        let mem = &index.mmap[..];
        for i in (3 * INDEX_ENTRY_BYTES)..(5 * INDEX_ENTRY_BYTES) {
            assert_eq!(0, mem[i], "Expected 0 at index {}", i);
        }
    }

    #[test]
    fn index_truncate_to_0() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 0, 128).unwrap();
        let mut buf = IndexBuf::new(5, 0);
        buf.push(0, 10);
        buf.push(1, 20);
        buf.push(2, 30);
        buf.push(3, 40);
        buf.push(4, 50);
        index.append(buf).unwrap();

        assert_eq!(5, index.next_offset());
        assert_eq!(5 * INDEX_ENTRY_BYTES, index.next_write_pos);

        // ensure we've zeroed the entries
        let mem = &index.mmap[..];
        for i in (1 * INDEX_ENTRY_BYTES)..(1 * INDEX_ENTRY_BYTES) {
            assert_eq!(0, mem[i], "Expected 0 at index {}", i);
        }
    }

    #[test]
    fn index_truncate_at_boundary() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let mut index = Index::new(&dir, 10u64, 128usize).unwrap();
        let mut buf = IndexBuf::new(5, 10u64);
        buf.push(10, 10);
        buf.push(11, 20);
        buf.push(12, 30);
        buf.push(13, 40);
        buf.push(14, 50);
        index.append(buf).unwrap();

        let file_len = index.truncate(14);
        assert_eq!(None, file_len);
        assert_eq!(15, index.next_offset());
        assert_eq!(5 * INDEX_ENTRY_BYTES, index.next_write_pos);
    }

    #[test]
    fn test_rehydrate_from_empty_index() {
        // Setup a test directory and a segment
        let dir = TestDir::new();
        let mut segment = Segment::new(&dir, 0, 1024).unwrap();

        // Write some data to the segment
        {
            let mut buf = MessageBuf::default();
            let tx_id = 1;
            buf.push(tx_id, "12345").unwrap();
            set_offsets(&mut buf, 0);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(2, meta.starting_position);

            // Append commit message
            let mut commit_buf = MessageBuf::default();
            commit_buf.push_commit(tx_id);
            let meta = segment.append(&commit_buf).unwrap();
            assert_eq!(33, meta.starting_position);
        }

        segment.flush().unwrap();

        // Create the index
        let mut index = Index::new(&dir, segment.starting_offset(), 128).unwrap();

        // Call the rehydrate method
        index.rehydrate(&segment, 100).unwrap();

        // Since we have one committed event, the index should have one entry
        assert_eq!(1 * INDEX_ENTRY_BYTES, index.next_write_pos);
        assert_eq!(1, index.next_offset());
        assert_eq!(index.read_entry(0), Some((0, 2)));
    }

    #[test]
    fn test_rehydrate_from_partial_index_starting_0() {
        let dir = TestDir::new();
        let mut segment = Segment::new(&dir, 0, 1024).unwrap();
        let mut index = Index::new(&dir, segment.starting_offset(), 128).unwrap();

        // Write some data to the segment
        {
            let tx_id = 1;

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "12345").unwrap();
            set_offsets(&mut buf, 0);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(2, meta.starting_position);

            let mut buf = IndexBuf::new(1, 0);
            buf.push(0, meta.starting_position as u32);
            index.append(buf).unwrap();

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "hello").unwrap();
            set_offsets(&mut buf, 1);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(33, meta.starting_position);

            let mut buf = IndexBuf::new(1, 0);
            buf.push(1, meta.starting_position as u32);
            index.append(buf).unwrap();

            // Append commit message
            let mut commit_buf = MessageBuf::default();
            commit_buf.push_commit(tx_id);
            segment.append(&commit_buf).unwrap();
        }

        segment.flush().unwrap();

        // Call the rehydrate method
        let result = index.rehydrate(&segment, 100);
        assert!(result.is_ok());

        index.flush().unwrap();

        // We only have two committed events
        assert_eq!(2 * INDEX_ENTRY_BYTES, index.next_write_pos);
        assert_eq!(2, index.next_offset());

        // Verify the index has been updated
        assert_eq!(index.read_entry(0), Some((0, 2)));
        assert_eq!(index.read_entry(1), Some((1, 33)));
        assert_eq!(index.read_entry(2), None);
    }

    #[test]
    fn test_rehydrate_from_partial_index_starting_100() {
        let dir = TestDir::new();
        let mut segment = Segment::new(&dir, 100, 1024).unwrap();
        let mut index = Index::new(&dir, segment.starting_offset(), 128).unwrap();

        // Write some data to the segment
        {
            let tx_id = 1;

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "12345").unwrap();
            set_offsets(&mut buf, 100);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(2, meta.starting_position);

            let mut buf = IndexBuf::new(1, 100);
            buf.push(100, meta.starting_position as u32);
            index.append(buf).unwrap();

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "hello").unwrap();
            set_offsets(&mut buf, 101);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(33, meta.starting_position);

            let mut buf = IndexBuf::new(1, 100);
            buf.push(101, meta.starting_position as u32);
            index.append(buf).unwrap();

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "world").unwrap();
            set_offsets(&mut buf, 102);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(64, meta.starting_position);

            let mut buf = MessageBuf::default();
            buf.push(tx_id, ":D").unwrap();
            set_offsets(&mut buf, 103);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(95, meta.starting_position);

            // Append commit message
            let mut commit_buf = MessageBuf::default();
            commit_buf.push_commit(tx_id);
            segment.append(&commit_buf).unwrap();
        }

        segment.flush().unwrap();
        index.flush().unwrap();

        // Call the rehydrate method
        let result = index.rehydrate(&segment, 100);
        assert!(result.is_ok());

        index.flush().unwrap();

        assert_eq!(4 * INDEX_ENTRY_BYTES, index.next_write_pos);
        assert_eq!(4 + 100, index.next_offset());

        // Verify the index has been updated
        assert_eq!(index.read_entry(0), Some((100, 2)));
        assert_eq!(index.read_entry(1), Some((101, 33)));
        assert_eq!(index.read_entry(2), Some((102, 64)));
        assert_eq!(index.read_entry(3), Some((103, 95)));
        assert_eq!(index.read_entry(4), None);
    }

    #[test]
    fn test_rehydrate_uncomitted_events() {
        let dir = TestDir::new();
        let mut segment = Segment::new(&dir, 100, 1024).unwrap();
        let mut index = Index::new(&dir, segment.starting_offset(), 128).unwrap();

        // Write some data to the segment
        {
            let tx_id = 1;

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "12345").unwrap();
            set_offsets(&mut buf, 100);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(2, meta.starting_position);

            let mut buf = IndexBuf::new(1, 100);
            buf.push(100, meta.starting_position as u32);
            index.append(buf).unwrap();

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "hello").unwrap();
            set_offsets(&mut buf, 101);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(33, meta.starting_position);

            let mut buf = IndexBuf::new(1, 100);
            buf.push(101, meta.starting_position as u32);
            index.append(buf).unwrap();

            // Append commit message
            let mut commit_buf = MessageBuf::default();
            commit_buf.push_commit(tx_id);
            segment.append(&commit_buf).unwrap();

            let tx_id = 2;

            let mut buf = MessageBuf::default();
            buf.push(tx_id, "world").unwrap();
            set_offsets(&mut buf, 102);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(90, meta.starting_position);

            let mut buf = MessageBuf::default();
            buf.push(tx_id, ":D").unwrap();
            set_offsets(&mut buf, 103);
            let meta = segment.append(&buf).unwrap();
            assert_eq!(121, meta.starting_position);
        }

        segment.flush().unwrap();

        // Call the rehydrate method
        let result = index.rehydrate(&segment, 100);
        assert!(result.is_ok());

        index.flush().unwrap();

        assert_eq!(2 * INDEX_ENTRY_BYTES, index.next_write_pos);
        assert_eq!(2 + 100, index.next_offset());

        // Verify the index has been updated
        assert_eq!(index.read_entry(0), Some((100, 2)));
        assert_eq!(index.read_entry(1), Some((101, 33)));
        // assert_eq!(index.read_entry(2), Some((102, 90)));
        // assert_eq!(index.read_entry(3), Some((103, 121)));
        assert_eq!(index.read_entry(2), None);
    }

    #[test]
    fn test_rehydrate_empty_segment() {
        // Setup a test directory and a segment
        let dir = TestDir::new();
        let segment = Segment::new(&dir, 0, 1024).unwrap();

        // Create the index
        let mut index = Index::new(&dir, segment.starting_offset(), 128).unwrap();

        // Call the rehydrate method
        let result = index.rehydrate(&segment, 100);
        assert!(result.is_ok());

        // Verify the index is empty
        assert!(index.is_empty());
        assert_eq!(0, index.next_write_pos);
        assert_eq!(0, index.next_offset());
    }
}
