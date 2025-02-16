use std::{
    borrow::Cow,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
};

use uuid::Uuid;

use super::{
    calculate_commit_crc32c, calculate_event_crc32c, BucketSegmentHeader, FlushedOffset,
    BUCKET_ID_SIZE, COMMIT_SIZE, CREATED_AT_SIZE, EVENT_HEADER_SIZE, MAGIC_BYTES, MAGIC_BYTES_SIZE,
    RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, VERSION_SIZE,
};

const PAGE_SIZE: usize = 4096; // Usually a page is 4KB on Linux
const READ_AHEAD_SIZE: usize = 64 * 1024; // 64 KB read ahead buffer
const READ_BUF_SIZE: usize = PAGE_SIZE - RECORD_HEADER_SIZE;

macro_rules! read_bytes {
    ($buf:expr, $pos:ident, $len:expr) => {{
        let pos = $pos;
        let len = $len;
        let buf = $buf[pos..pos + len].try_into().unwrap();
        #[allow(unused_assignments)]
        {
            $pos += len;
        }
        buf
    }};
}

#[derive(Debug)]
pub struct BucketSegmentReader {
    file: File,
    header_buf: [u8; COMMIT_SIZE],
    body_buf: [u8; READ_BUF_SIZE],
    flushed_offset: FlushedOffset,

    // Read-ahead buffer for sequential reads
    read_ahead_buf: Vec<u8>,
    read_ahead_offset: u64, // File offset of the buffer start
    read_ahead_pos: usize,  // Current read position in buffer
    read_ahead_valid_len: usize,
}

impl BucketSegmentReader {
    /// Opens a segment as read only.
    pub fn open(path: impl AsRef<Path>, flushed_offset: FlushedOffset) -> io::Result<Self> {
        // On OSX, gives ~5% better performance for both random and sequential reads
        const O_DIRECT: i32 = 0o0040000;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(O_DIRECT)
            .open(path)?;
        let header_buf = [0u8; COMMIT_SIZE];
        let body_buf = [0u8; READ_BUF_SIZE];

        Ok(BucketSegmentReader {
            file,
            header_buf,
            body_buf,
            flushed_offset,
            read_ahead_buf: Vec::new(),
            read_ahead_offset: 0,
            read_ahead_pos: 0,
            read_ahead_valid_len: 0,
        })
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(BucketSegmentReader {
            file: self.file.try_clone()?,
            header_buf: self.header_buf,
            body_buf: self.body_buf,
            flushed_offset: self.flushed_offset.clone(),
            read_ahead_buf: Vec::with_capacity(READ_AHEAD_SIZE),
            read_ahead_offset: self.read_ahead_offset,
            read_ahead_pos: self.read_ahead_pos,
            read_ahead_valid_len: self.read_ahead_valid_len,
        })
    }

    /// Reads the segments header.
    pub fn read_segment_header(&mut self) -> io::Result<BucketSegmentHeader> {
        let mut header_bytes = [0u8; VERSION_SIZE + BUCKET_ID_SIZE + CREATED_AT_SIZE];

        self.file.seek(SeekFrom::Start(MAGIC_BYTES_SIZE as u64))?;
        self.file.read_exact(&mut header_bytes)?;

        let version_bytes = header_bytes[0..VERSION_SIZE]
            .try_into()
            .expect("Slice has correct length");
        let version = u16::from_le_bytes(version_bytes);

        let bucket_id_bytes = header_bytes[VERSION_SIZE..VERSION_SIZE + BUCKET_ID_SIZE]
            .try_into()
            .expect("Slice has correct length");
        let bucket_id = u16::from_le_bytes(bucket_id_bytes);

        let created_at_bytes = header_bytes
            [VERSION_SIZE + BUCKET_ID_SIZE..VERSION_SIZE + BUCKET_ID_SIZE + CREATED_AT_SIZE]
            .try_into()
            .expect("Slice has correct length");
        let created_at = u64::from_le_bytes(created_at_bytes);

        Ok(BucketSegmentHeader {
            version,
            bucket_id,
            created_at,
        })
    }

    pub fn iter(&mut self) -> BucketSegmentIter {
        BucketSegmentIter {
            reader: self,
            offset: SEGMENT_HEADER_SIZE as u64,
        }
    }

    pub fn iter_from(&mut self, start_offset: u64) -> BucketSegmentIter {
        BucketSegmentIter {
            reader: self,
            offset: start_offset,
        }
    }

    /// Validates the segments magic bytes.
    pub fn validate_magic_bytes(&mut self) -> io::Result<bool> {
        let mut magic_bytes = [0u8; MAGIC_BYTES_SIZE];

        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut magic_bytes)?;

        Ok(u32::from_le_bytes(magic_bytes) == MAGIC_BYTES)
    }

    /// Reads the segments version.
    pub fn read_version(&mut self) -> io::Result<u16> {
        let mut version_bytes = [0u8; VERSION_SIZE];

        self.file.seek(SeekFrom::Start(MAGIC_BYTES_SIZE as u64))?;
        self.file.read_exact(&mut version_bytes)?;

        Ok(u16::from_le_bytes(version_bytes))
    }

    /// Reads the segments bucket ID.
    pub fn read_bucket_id(&mut self) -> io::Result<u16> {
        let mut bucket_id_bytes = [0u8; BUCKET_ID_SIZE];

        self.file
            .seek(SeekFrom::Start((MAGIC_BYTES_SIZE + VERSION_SIZE) as u64))?;
        self.file.read_exact(&mut bucket_id_bytes)?;

        Ok(u16::from_le_bytes(bucket_id_bytes))
    }

    /// Reads the segments created at date.
    pub fn read_created_at(&mut self) -> io::Result<u64> {
        let mut created_at_bytes = [0u8; CREATED_AT_SIZE];

        self.file.seek(SeekFrom::Start(
            (MAGIC_BYTES_SIZE + VERSION_SIZE + BUCKET_ID_SIZE) as u64,
        ))?;
        self.file.read_exact(&mut created_at_bytes)?;

        Ok(u64::from_le_bytes(created_at_bytes))
    }

    /// Reads a record at the given offset, returning either an event or commit.
    ///
    /// If sequential is `true`, the read will be optimized for future sequential reads.
    /// Random reads should have `sequential` set to `false`.
    ///
    /// Borrowed data will be returned in the record where possible. If the event length exceeds 4KB, then the event
    /// will be read directly from the file, and the returned `Record` will contain owned data.
    pub fn read_record(&mut self, mut offset: u64, sequential: bool) -> io::Result<Option<Record>> {
        // This is the only check needed. We don't need to check for the event body,
        // since if the offset supports this header read, then the event body would have also been written too for the flush.
        if offset + COMMIT_SIZE as u64 > self.flushed_offset.load() {
            return Ok(None);
        }

        let header_buf = if sequential {
            self.read_from_read_ahead(offset, COMMIT_SIZE)?
        } else {
            self.file
                .read_exact_at(&mut self.header_buf[..COMMIT_SIZE], offset)?;
            self.header_buf.as_slice()
        };
        offset += COMMIT_SIZE as u64;

        let record_header = RecordHeader::from_bytes(header_buf);

        if record_header.record_type == 0 {
            offset -= 4;
            self.read_event_body(record_header, offset, sequential)
                .map(Some)
        } else if record_header.record_type == 1 {
            let event_count = u32::from_le_bytes(
                header_buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + 4]
                    .try_into()
                    .unwrap(),
            );
            self.read_commit_body(record_header, event_count).map(Some)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown record type",
            ))
        }
    }

    fn read_event_body(
        &mut self,
        record_header: RecordHeader,
        mut offset: u64,
        sequential: bool,
    ) -> io::Result<Record> {
        let length = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
        let header_buf = if sequential {
            self.read_from_read_ahead(offset, length)?
        } else {
            self.file
                .read_exact_at(&mut self.header_buf[..length], offset)?;
            self.header_buf.as_slice()
        };
        offset += length as u64;

        let event_header = EventHeader::from_bytes(header_buf);
        let body_len = event_header.body_len();
        let body = if sequential {
            let body_buf = self.read_from_read_ahead(offset, body_len)?;
            EventBody::from_bytes(&event_header, body_buf)?
        } else if body_len > self.body_buf.len() {
            let mut body_buf = vec![0u8; body_len];
            self.file.read_exact_at(&mut body_buf, offset)?;
            EventBody::from_bytes_owned(&event_header, body_buf)?
        } else {
            self.file
                .read_exact_at(&mut self.body_buf[..body_len], offset)?;
            EventBody::from_bytes(&event_header, self.body_buf.as_slice())?
        };

        validate_and_combine_event(record_header, event_header, body)
    }

    fn read_commit_body(
        &self,
        record_header: RecordHeader,
        event_count: u32,
    ) -> io::Result<Record> {
        let new_crc32c = calculate_commit_crc32c(
            &record_header.transaction_id,
            record_header.timestamp,
            event_count,
        );
        if record_header.crc32c != new_crc32c {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mismatching commit crc32c hash",
            ));
        }

        Ok(Record::Commit {
            transaction_id: record_header.transaction_id,
            timestamp: record_header.timestamp,
            event_count,
        })
    }

    fn fill_read_ahead(&mut self, offset: u64, mut length: usize) -> io::Result<()> {
        let end_offset = offset + length as u64;

        // Set the new read-ahead offset aligned to 64KB
        self.read_ahead_offset = offset - (offset % READ_AHEAD_SIZE as u64);
        self.read_ahead_pos = 0;
        length = (end_offset - self.read_ahead_offset) as usize;

        // If the requested read is larger than READ_AHEAD_SIZE, expand the buffer to the next leargest interval of 4096
        let required_size = (length.max(READ_AHEAD_SIZE) + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);

        // Resize buffer if necessary
        if self.read_ahead_buf.len() != required_size {
            self.read_ahead_buf.resize(required_size, 0);
            self.read_ahead_buf.shrink_to_fit();
        }

        let mut total_read = 0;
        while total_read < required_size {
            let bytes_read = self.file.read_at(
                &mut self.read_ahead_buf[total_read..],
                self.read_ahead_offset + total_read as u64,
            )?;
            if bytes_read == 0 {
                break; // EOF reached
            }
            total_read += bytes_read;
        }

        self.read_ahead_valid_len = total_read; // Track the actual valid bytes

        Ok(())
    }

    fn read_from_read_ahead(&mut self, offset: u64, length: usize) -> io::Result<&[u8]> {
        let end_offset = offset + length as u64;

        // If offset is within the valid read-ahead range
        if offset >= self.read_ahead_offset
            && end_offset <= (self.read_ahead_offset + self.read_ahead_valid_len as u64)
        {
            let start = (offset - self.read_ahead_offset) as usize;
            return Ok(&self.read_ahead_buf[start..start + length]);
        }

        // Fill the read-ahead buffer for the requested offset & length
        self.fill_read_ahead(offset, length)?;

        // Ensure we now have enough valid data
        if offset < self.read_ahead_offset
            || end_offset > (self.read_ahead_offset + self.read_ahead_valid_len as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "requested data exceeds available read-ahead buffer",
            ));
        }

        let start = (offset - self.read_ahead_offset) as usize;
        Ok(&self.read_ahead_buf[start..start + length])
    }
}

pub struct BucketSegmentIter<'a> {
    reader: &'a mut BucketSegmentReader,
    offset: u64,
}

impl BucketSegmentIter<'_> {
    pub fn next_record(&mut self) -> io::Result<Option<(u64, Record)>> {
        match self.reader.read_record(self.offset, true) {
            Ok(Some(record)) => {
                let record_offset = self.offset;
                let record_len = match &record {
                    Record::Event {
                        stream_id,
                        event_name,
                        metadata,
                        payload,
                        ..
                    } => {
                        EVENT_HEADER_SIZE as u64
                            + stream_id.len() as u64
                            + event_name.len() as u64
                            + metadata.len() as u64
                            + payload.len() as u64
                    }
                    Record::Commit { .. } => COMMIT_SIZE as u64,
                };
                self.offset += record_len;
                Ok(Some((record_offset, record)))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record<'a> {
    Event {
        event_id: Uuid,
        correlation_id: Uuid,
        transaction_id: Uuid,
        stream_version: u64,
        timestamp: u64,
        stream_id: Cow<'a, str>,
        event_name: Cow<'a, str>,
        metadata: Cow<'a, [u8]>,
        payload: Cow<'a, [u8]>,
    },
    Commit {
        transaction_id: Uuid,
        timestamp: u64,
        event_count: u32,
    },
}

impl<'a> Record<'a> {
    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::Event {
                event_id,
                correlation_id,
                transaction_id,
                stream_version,
                timestamp,
                stream_id,
                event_name,
                metadata,
                payload,
            } => Record::Event {
                event_id,
                correlation_id,
                transaction_id,
                stream_version,
                timestamp,
                stream_id: Cow::Owned(stream_id.into_owned()),
                event_name: Cow::Owned(event_name.into_owned()),
                metadata: Cow::Owned(metadata.into_owned()),
                payload: Cow::Owned(payload.into_owned()),
            },
            Record::Commit {
                transaction_id,
                timestamp,
                event_count,
            } => Record::Commit {
                transaction_id,
                timestamp,
                event_count,
            },
        }
    }
}

struct RecordHeader {
    event_id: Uuid,
    transaction_id: Uuid,
    timestamp: u64,
    crc32c: u32,
    record_type: u8,
}

impl RecordHeader {
    fn from_bytes(buf: &[u8]) -> Self {
        let mut pos = 0;
        let event_id = Uuid::from_bytes(read_bytes!(buf, pos, 16));
        let transaction_id = Uuid::from_bytes(read_bytes!(buf, pos, 16));
        let timestamp = u64::from_le_bytes(read_bytes!(buf, pos, 8));
        let crc32c = u32::from_le_bytes(read_bytes!(buf, pos, 4));
        let record_type = (timestamp & 0b1) as u8;
        RecordHeader {
            event_id,
            transaction_id,
            timestamp,
            crc32c,
            record_type,
        }
    }
}

struct EventHeader {
    stream_version: u64,
    correlation_id: Uuid,
    stream_id_len: usize,
    event_name_len: usize,
    metadata_len: usize,
    payload_len: usize,
}

impl EventHeader {
    fn from_bytes(buf: &[u8]) -> Self {
        let mut pos = 0;
        EventHeader {
            stream_version: u64::from_le_bytes(read_bytes!(buf, pos, 8)),
            correlation_id: Uuid::from_bytes(read_bytes!(buf, pos, 16)),
            stream_id_len: u16::from_le_bytes(read_bytes!(buf, pos, 2)) as usize,
            event_name_len: u16::from_le_bytes(read_bytes!(buf, pos, 2)) as usize,
            metadata_len: u32::from_le_bytes(read_bytes!(buf, pos, 4)) as usize,
            payload_len: u32::from_le_bytes(read_bytes!(buf, pos, 4)) as usize,
        }
    }

    fn body_len(&self) -> usize {
        self.stream_id_len + self.event_name_len + self.metadata_len + self.payload_len
    }
}

struct EventBody<'a> {
    stream_id: Cow<'a, str>,
    event_name: Cow<'a, str>,
    metadata: Cow<'a, [u8]>,
    payload: Cow<'a, [u8]>,
}

impl<'a> EventBody<'a> {
    fn from_bytes(event_header: &EventHeader, buf: &'a [u8]) -> io::Result<EventBody<'a>> {
        let mut pos = 0;

        let stream_id = std::str::from_utf8(&buf[pos..pos + event_header.stream_id_len])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 stream id"))?;
        pos += event_header.stream_id_len;

        let event_name = std::str::from_utf8(&buf[pos..pos + event_header.event_name_len])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 event name"))?;
        pos += event_header.event_name_len;

        let metadata = &buf[pos..pos + event_header.metadata_len];
        pos += event_header.metadata_len;

        let payload = &buf[pos..pos + event_header.payload_len];

        Ok(EventBody {
            stream_id: Cow::Borrowed(stream_id),
            event_name: Cow::Borrowed(event_name),
            metadata: Cow::Borrowed(metadata),
            payload: Cow::Borrowed(payload),
        })
    }

    fn from_bytes_owned(
        event_header: &EventHeader,
        mut buf: Vec<u8>,
    ) -> io::Result<EventBody<'static>> {
        let mut pos = 0;

        let stream_id = std::str::from_utf8(&buf[pos..pos + event_header.stream_id_len])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 stream id"))?
            .to_owned();
        pos += event_header.stream_id_len;

        let event_name = std::str::from_utf8(&buf[pos..pos + event_header.event_name_len])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 event name"))?
            .to_owned();
        pos += event_header.event_name_len;

        let metadata = buf[pos..pos + event_header.metadata_len].to_owned();
        pos += event_header.metadata_len;

        buf.drain(..pos);

        Ok(EventBody {
            stream_id: Cow::Owned(stream_id),
            event_name: Cow::Owned(event_name),
            metadata: Cow::Owned(metadata),
            payload: Cow::Owned(buf),
        })
    }
}

fn validate_and_combine_event(
    record_header: RecordHeader,
    event_header: EventHeader,
    body: EventBody,
) -> io::Result<Record> {
    let new_crc32c = calculate_event_crc32c(
        &record_header.event_id,
        &record_header.transaction_id,
        record_header.timestamp,
        event_header.stream_version,
        &event_header.correlation_id,
        body.stream_id.as_bytes(),
        body.event_name.as_bytes(),
        &body.metadata,
        &body.payload,
    );
    if record_header.crc32c != new_crc32c {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mismatching event crc32c hash",
        ));
    }

    Ok(Record::Event {
        event_id: record_header.event_id,
        correlation_id: event_header.correlation_id,
        transaction_id: record_header.transaction_id,
        stream_version: event_header.stream_version,
        timestamp: record_header.timestamp,
        stream_id: body.stream_id,
        event_name: body.event_name,
        metadata: body.metadata,
        payload: body.payload,
    })
}
