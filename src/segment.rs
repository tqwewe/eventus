use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, File};
use crc::crc32::checksum_ieee;
use byteorder::{BigEndian, ByteOrder};
use std::io::{self, Write, Read, BufReader, Seek, SeekFrom};

/// Number of bytes contained in the base name of the file.
pub static SEGMENT_FILE_NAME_LEN: usize = 20;
/// File extension for the segment file.
pub static SEGMENT_FILE_NAME_EXTENSION: &'static str = "log";

#[derive(Debug)]
pub enum MessageError {
    IoError(io::Error),
    InvalidCRC,
}

impl From<io::Error> for MessageError {
    fn from(e: io::Error) -> MessageError {
        MessageError::IoError(e)
    }
}

macro_rules! read_n {
    ($reader:expr, $buf:expr, $size:expr, $err_msg:expr) => ({

        match $reader.read(&mut $buf) {
            Ok(s) if s == $size => (),
            Ok(_) => return Err(
                MessageError::IoError(
                    io::Error::new(io::ErrorKind::UnexpectedEof, $err_msg))),
            Err(e) => return Err(MessageError::IoError(e)),
        }
    })
}


/// Messages are appended to the log with the following encoding:
///
/// | Bytes     | Encoding       | Value        |
/// | --------- | -------------- | ------------ |
/// | 0-7       | Big Endian u64 | Offset       |
/// | 8-11      | Big Endian u32 | Payload Size |
/// | 12-15     | Big Endian u32 | CRC32 (IEEE) |
/// | 16+       |                | Payload      |
///
#[derive(Debug)]
pub struct Message<'a> {
    bytes: &'a [u8],
}

impl<'a> Message<'a> {
    /// IEEE CRC32 of the payload.
    #[inline]
    pub fn crc(&self) -> u32 {
        BigEndian::read_u32(&self.bytes[12..16])
    }

    /// Size of the payload.
    #[inline]
    pub fn size(&self) -> u32 {
        BigEndian::read_u32(&self.bytes[8..12])
    }

    /// Offset of the message in the log.
    #[inline]
    pub fn offset(&self) -> u64 {
        BigEndian::read_u64(&self.bytes[0..8])
    }

    /// Payload of the message.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.bytes[16..]
    }
}

pub struct MessageSet {
    bytes: Vec<u8>,
    size: usize,
}

impl MessageSet {
    pub fn new() -> MessageSet {
        MessageSet {
            bytes: Vec::new(),
            size: 0,
        }
    }

    /// Reads a single message.
    fn read<R>(&mut self, reader: &mut R) -> Result<(), MessageError>
        where R: Read
    {
        let mut offset_buf= vec![0; 8];
        read_n!(reader, offset_buf, 8, "Unable to read offset");
        let mut size_buf = vec![0; 4];
        read_n!(reader, size_buf, 4, "Unable to read size");
        let mut crc_buf = vec![0; 4];
        read_n!(reader, crc_buf, 4, "Unable to read CRC");

        let size = BigEndian::read_u32(&size_buf) as usize;
        let crc = BigEndian::read_u32(&crc_buf);

        let mut bytes = vec![0; size];
        read_n!(reader, bytes, size, "Unable to read message payload");

        let payload_crc = checksum_ieee(&bytes);
        if payload_crc != crc {
            return Err(MessageError::InvalidCRC);
        }

        self.bytes.extend(offset_buf);
        self.bytes.extend(size_buf);
        self.bytes.extend(crc_buf);
        self.bytes.extend(bytes);
        self.size += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn iter<'a>(&'a self) -> MessageIter<'a> {
        MessageIter {
            bytes: &self.bytes,
            offset: 0,
        }
    }

    #[cfg(test)]
    pub fn copy_bytes(&self) -> Vec<u8> {
        self.bytes.iter().cloned().collect()
    }
}

pub struct MessageIter<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = Message<'a>;

    fn next(&mut self) -> Option<Message<'a>> {
        if self.offset + 16 >= self.bytes.len() {
            return None;
        }

        let off_slice = &self.bytes[self.offset..];
        let size = BigEndian::read_u32(&off_slice[8..12]) as usize;
        info!("Size {} bytes", size);
        let message_slice = &off_slice[0..16+size];
        self.offset += 16 + size;
        Some(Message {
            bytes: message_slice,
        })
    }
}

pub struct MessageBuf {
    bytes: Vec<u8>,
    size: usize,
    byte_offsets: Vec<usize>,
}

impl MessageBuf {
    pub fn new() -> MessageBuf {
        MessageBuf {
            bytes: Vec::new(),
            size: 0,
            byte_offsets: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    /// Adds a new message with a given payload and offset.
    pub fn push(&mut self, payload: &[u8]) {
        let start_len = self.bytes.len();

        // blank offset, expect the log to set the offsets
        let mut buf = vec![0; 8];
        self.bytes.extend_from_slice(&buf);

        BigEndian::write_u32(&mut buf[0..4], payload.len() as u32);
        self.bytes.extend_from_slice(&buf[0..4]);

        BigEndian::write_u32(&mut buf[0..4], checksum_ieee(payload));
        self.bytes.extend_from_slice(&buf[0..4]);

        self.bytes.extend_from_slice(payload);

        self.size += 1;
        self.byte_offsets.push(start_len);
    }

    fn set_offsets(&mut self, starting_offset: u64) {
        for (i, pos) in self.byte_offsets.iter().enumerate() {
            BigEndian::write_u64(&mut self.bytes[*pos..*pos+8], (i as u64) + starting_offset);
        }
    }

    fn create_metadata(&self, starting_offset: u64, base_file_pos: u32) -> Vec<LogEntryMetadata> {
        self.byte_offsets.iter().enumerate().map(move |(i, pos)| {
            LogEntryMetadata {
                offset: starting_offset + (i as u64),
                file_pos: (*pos as u32) + base_file_pos,
            }
        }).collect()
    }

    #[cfg(test)]
    pub fn into_msg_set(self) -> MessageSet {
        MessageSet {
            bytes: self.bytes,
            size: self.size,
        }
    }
}

impl<'a> From<&'a [u8]> for MessageBuf {
    fn from(s: &'a [u8]) -> MessageBuf {
        let mut buf = MessageBuf::new();
        buf.push(s);
        buf
    }
}

impl<'a> From<&'a str> for MessageBuf {
    fn from(s: &'a str) -> MessageBuf {
        let mut buf = MessageBuf::new();
        buf.push(s.as_bytes());
        buf
    }
}

enum SegmentMode {
    ReadWrite {
        /// current file position for the write
        write_pos: usize,

        /// Next offset of the log
        next_offset: u64,

        /// Maximum number of bytes permitted to be appended to the log
        max_bytes: usize,
    },
    Read,
}


/// A segment is a portion of the commit log. Segments are append-only logs written
/// until the maximum size is reached.
pub struct Segment {
    /// File descriptor
    file: File,

    mode: SegmentMode,
    has_read: bool,

    /// Base offset of the log
    base_offset: u64,
}

#[derive(Debug)]
pub enum SegmentAppendError {
    LogFull,
    IoError(io::Error),
}

impl From<io::Error> for SegmentAppendError {
    #[inline]
    fn from(e: io::Error) -> SegmentAppendError {
        SegmentAppendError::IoError(e)
    }
}

/// Holds the pair of offset written to file position in the segment file.
#[derive(Copy, Clone, Debug)]
pub struct LogEntryMetadata {
    offset: u64,
    file_pos: u32,
}

impl LogEntryMetadata {
    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    pub fn file_pos(&self) -> u32 {
        self.file_pos
    }
}

/// Batch size limitation on read.
pub enum ReadLimit {
    /// Limit the number of bytes read from the log. This is recommended.
    Bytes(usize),

    /// Limit the number of messages read from the log.
    Messages(usize),
}


impl Segment {
    pub fn new<P>(log_dir: P, base_offset: u64, max_bytes: usize) -> io::Result<Segment>
        where P: AsRef<Path>
    {
        let log_path = {
            // the log is of the form BASE_OFFSET.log
            let mut path_buf = PathBuf::new();
            path_buf.push(&log_dir);
            path_buf.push(format!("{:020}", base_offset));
            path_buf.set_extension(SEGMENT_FILE_NAME_EXTENSION);
            path_buf
        };

        let f = OpenOptions::new().write(true)
            .read(true)
            .create_new(true)
            .append(true)
            .open(&log_path)?;

        Ok(Segment {
            file: f,

            mode: SegmentMode::ReadWrite {
                write_pos: 0,
                next_offset: base_offset,
                max_bytes: max_bytes,
            },
            has_read: false,

            base_offset: base_offset,
        })
    }

    pub fn open<P>(seg_path: P) -> io::Result<Segment>
        where P: AsRef<Path>
    {
        let seg_file = OpenOptions::new().read(true)
            .write(false)
            .append(false)
            .open(&seg_path)?;


        let filename = seg_path.as_ref().file_name().unwrap().to_str().unwrap();
        let base_offset = match u64::from_str_radix(&filename[0..SEGMENT_FILE_NAME_LEN], 10) {
            Ok(v) => v,
            Err(_) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData,
                                          "Segment file name does not parse as u64"))
            }
        };


        Ok(Segment {
            file: seg_file,
            mode: SegmentMode::Read,
            has_read: false,
            base_offset: base_offset,
        })
    }

    // TODO: doesn't make sense
    pub fn next_offset(&self) -> u64 {
        match self.mode {
            SegmentMode::ReadWrite { next_offset, .. } => next_offset,
            _ => 0,
        }
    }

    #[inline]
    pub fn starting_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn append(&mut self, payload: &mut MessageBuf) -> Result<Vec<LogEntryMetadata>, SegmentAppendError> {
        let (write_pos, off, max_bytes) = match self.mode {
            SegmentMode::ReadWrite { write_pos, next_offset, max_bytes } => {
                (write_pos, next_offset, max_bytes)
            }
            _ => return Err(SegmentAppendError::LogFull),
        };

        payload.set_offsets(off);

        // ensure we have the capacity
        if payload.bytes.len() + write_pos > max_bytes {
            return Err(SegmentAppendError::LogFull);
        }

        // move cursor back to write position if we have moved the
        // cursor due to a read
        if self.has_read {
            self.file.seek(SeekFrom::Start(write_pos as u64))?;
            self.has_read = false;
        }

        self.file.write_all(&payload.bytes)?;
        self.mode = SegmentMode::ReadWrite {
            write_pos: write_pos + payload.bytes.len(),
            next_offset: off + 1,
            max_bytes: max_bytes,
        };
        Ok(payload.create_metadata(off, write_pos as u32))
    }

    // TODO: async flush strategy
    pub fn flush_sync(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    fn seek(&mut self, file_pos: u32) -> io::Result<()> {
        self.has_read = true;
        self.file.seek(SeekFrom::Start(file_pos as u64))?;
        Ok(())
    }

    pub fn read(&mut self, file_pos: u32, limit: ReadLimit) -> Result<MessageSet, MessageError> {
        self.seek(file_pos)?;

        let mut buf_reader = match limit {
            ReadLimit::Bytes(n) => BufReader::with_capacity(n, &mut self.file),
            _ => BufReader::new(&mut self.file),
        };

        let mut msgs = MessageSet::new();

        loop {
            match msgs.read(&mut buf_reader) {
                Ok(()) => {
                    match limit {
                        ReadLimit::Messages(l) if l <= msgs.len() => return Ok(msgs),
                        _ => {}
                    }
                }
                // EOF counts as an end to the stream, thus we're done fetching messages
                Err(MessageError::IoError(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Ok(msgs)
                }
                Err(e) => return Err(e),
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use super::super::testutil::*;
    use std::io;
    use std::path::PathBuf;
    use env_logger;

    #[test]
    fn message_construction() {
        env_logger::init().unwrap_or(());
        //let msg = Message::new(b"123456789", 1234567u64);
        let mut msg_buf = MessageBuf::new();
        msg_buf.push(b"123456789");
        let msg_set = msg_buf.into_msg_set();
        let mut msg_it = msg_set.iter();
        {
            let msg = msg_it.next().unwrap();
            assert_eq!(msg.payload(), b"123456789");
            assert_eq!(msg.crc(), 0xcbf43926);
            assert_eq!(msg.size(), 9u32);
        }
        assert!(msg_it.next().is_none());
    }

    #[test]
    fn message_read() {
        let mut buf = MessageBuf::new();
        buf.push(b"123456789");
        let bytes = buf.into_msg_set().copy_bytes();

        let mut buf_reader = io::BufReader::new(bytes.as_slice());

        let mut reader = MessageSet::new();
        let read_msg_result = reader.read(&mut buf_reader);
        assert!(read_msg_result.is_ok(), "result = {:?}", read_msg_result);

        let read_msg = reader.iter().next().unwrap();
        assert_eq!(read_msg.payload(), b"123456789");
        assert_eq!(read_msg.crc(), 0xcbf43926);
        assert_eq!(read_msg.size(), 9u32);
    }


    #[test]
    fn message_read_invalid_crc() {
        let mut buf = MessageBuf::new();
        buf.push(b"123456789");
        let mut msg = buf.into_msg_set().copy_bytes();
        // mess with the payload such that the CRC does not match
        let last_ind = msg.len() - 1;
        msg[last_ind] += 1u8;

        let mut buf_reader = io::BufReader::new(msg.as_slice());

        let mut reader = MessageSet::new();
        let read_msg_result = reader.read(&mut buf_reader);
        let matches_invalid_crc = match read_msg_result {
            Err(MessageError::InvalidCRC) => true,
            _ => false,
        };
        assert!(matches_invalid_crc,
                "Invalid result, not CRC error. Result = {:?}",
                read_msg_result);
    }


    #[test]
    fn message_read_invalid_payload_length() {
        let mut buf = MessageBuf::new();
        buf.push(b"123456789");
        let mut msg = buf.into_msg_set().copy_bytes();
        // pop the last byte
        msg.pop();

        let mut buf_reader = io::BufReader::new(msg.as_slice());
        let mut msg_reader = MessageSet::new();
        let read_msg_result = msg_reader.read(&mut buf_reader);
        let matches_invalid_crc = match read_msg_result {
            Err(MessageError::IoError(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => true,
            _ => false,
        };
        assert!(matches_invalid_crc,
                "Invalid result, not CRC error. Result = {:?}",
                read_msg_result);
    }


    #[test]
    pub fn log_append() {
        let path = TestDir::new();
        let mut f = Segment::new(path, 0, 1024).unwrap();

        {
            let mut buf = MessageBuf::new();
            buf.push(b"12345");
            let meta = f.append(&mut buf).unwrap();

            assert_eq!(1, meta.len());
            let p0 = meta.iter().next().unwrap();
            assert_eq!(p0.offset(), 0);
            assert_eq!(p0.file_pos(), 0);
        }

        {
            let mut buf = MessageBuf::new();
            buf.push(b"66666");
            buf.push(b"77777");
            let meta = f.append(&mut buf).unwrap();
            assert_eq!(2, meta.len());

            let mut it = meta.iter();
            let p0 = it.next().unwrap();
            assert_eq!(p0.offset(), 1);
            assert_eq!(p0.file_pos(), 21);

            let p1 = it.next().unwrap();
            assert_eq!(p1.offset(), 2);
            assert_eq!(p1.file_pos(), 42);
        }

        f.flush_sync().unwrap();
    }

    #[test]
    pub fn log_open() {
        let log_dir = TestDir::new();

        {
            let mut f = Segment::new(&log_dir, 0, 1024).unwrap();
            let mut buf = MessageBuf::new();
            buf.push(b"12345");
            buf.push(b"66666");
            f.append(&mut buf).unwrap();
            f.flush_sync().unwrap();
        }

        // open it
        {
            let mut path_buf = PathBuf::new();
            path_buf.push(&log_dir);
            path_buf.push(format!("{:020}", 0));
            path_buf.set_extension(SEGMENT_FILE_NAME_EXTENSION);

            let res = Segment::open(&path_buf);
            assert!(res.is_ok(), "Err {:?}", res.err());

            let f = res.unwrap();
            assert_eq!(0, f.starting_offset());
        }
    }


    #[test]
    pub fn log_read() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        {
            let mut buf = MessageBuf::new();
            buf.push(b"0123456789");
            buf.push(b"aaaaaaaaaa");
            buf.push(b"abc");
            f.append(&mut buf).unwrap();
        }

        let msgs = f.read(0, ReadLimit::Messages(10)).unwrap();
        assert_eq!(3, msgs.len());

        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(i as u64, m.offset());
        }
    }

    #[test]
    pub fn log_read_with_msg_limit() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        {
            let mut buf = MessageBuf::new();
            buf.push(b"0123456789");
            buf.push(b"aaaaaaaaaa");
            buf.push(b"abc");
            f.append(&mut buf).unwrap();
        }

        let msgs = f.read(0, ReadLimit::Messages(2)).unwrap();
        assert_eq!(2, msgs.len());
    }


    #[test]
    pub fn log_read_with_size_limit() {
        let log_dir = TestDir::new();
        let mut f = Segment::new(&log_dir, 0, 1024).unwrap();

        let meta = {
            let mut buf = MessageBuf::new();
            buf.push(b"0123456789");
            buf.push(b"aaaaaaaaaa");
            buf.push(b"abc");
            f.append(&mut buf).unwrap()
        };

        // byte max contains message 0, but not the entirety of message 1
        let msgs = f.read(0,
                  ReadLimit::Bytes((meta[1].file_pos() + 1) as usize))
            .unwrap();
        assert_eq!(1, msgs.len());
    }


    #[bench]
    fn bench_segment_append(b: &mut Bencher) {
        let path = TestDir::new();

        let mut seg = Segment::new(path, 100u64, 100 * 1024 * 1024).unwrap();
        let payload = b"01234567891011121314151617181920";

        b.iter(|| {
            let mut buf = MessageBuf::new();
            buf.push(payload);
            seg.append(&mut buf).unwrap();
        });
    }
}
