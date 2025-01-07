use std::collections::BTreeMap;
use std::{
    fs, io,
    mem::{replace, swap},
};

use tracing::{debug, error, info, trace, warn};

use crate::stream_index::{StreamIndex, STREAM_INDEX_FILE_NAME_EXTENSION};

use super::{index::*, segment::*, LogOptions, Offset};

pub struct SegmentSet {
    pub segment: Segment,
    pub index: Index,
    pub stream_index: StreamIndex,
}

impl SegmentSet {
    pub fn new(opts: &LogOptions, next_offset: u64) -> io::Result<Self> {
        let segment = Segment::new(&opts.log_dir, next_offset, opts.log_max_bytes)?;
        let index = Index::new(&opts.log_dir, next_offset, opts.index_max_bytes)?;
        let stream_index = StreamIndex::new(&opts.log_dir, next_offset, opts.log_max_entries)?;

        Ok(SegmentSet {
            segment,
            index,
            stream_index,
        })
    }
}

pub struct FileSet {
    active: SegmentSet,
    closed: BTreeMap<u64, SegmentSet>,
    opts: LogOptions,
}

impl FileSet {
    pub fn load_log(opts: LogOptions) -> io::Result<FileSet> {
        let mut segments = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        let mut stream_indexes = BTreeMap::new();

        let files = fs::read_dir(&opts.log_dir)?
            // ignore Err results
            .filter_map(Result::ok)
            // ignore directories
            .filter(|err| err.metadata().map(|m| m.is_file()).unwrap_or(false));

        for f in files {
            match f.path().extension() {
                Some(ext) if SEGMENT_FILE_NAME_EXTENSION.eq(ext) => {
                    let segment = match Segment::open(f.path(), opts.log_max_bytes) {
                        Ok(seg) => seg,
                        Err(err) => {
                            error!("Unable to open segment {:?}: {}", f.path(), err);
                            return Err(err);
                        }
                    };

                    let offset = segment.starting_offset();
                    segments.insert(offset, segment);
                }
                Some(ext) if INDEX_FILE_NAME_EXTENSION.eq(ext) => {
                    let index = match Index::open(f.path()) {
                        Ok(index) => index,
                        Err(err) => {
                            error!("Unable to open index {:?}: {}", f.path(), err);
                            return Err(err);
                        }
                    };

                    let offset = index.starting_offset();
                    indexes.insert(offset, index);
                    // TODO: fix missing index updates (crash before write to index)
                }
                Some(ext) if STREAM_INDEX_FILE_NAME_EXTENSION.eq(ext) => {
                    let stream_index = match StreamIndex::open(&f.path()) {
                        Ok(stream_index) => stream_index,
                        Err(err) => {
                            error!("Unable to open stream index {:?}: {}", f.path(), err);
                            return Err(err);
                        }
                    };

                    let offset = stream_index.starting_offset();
                    stream_indexes.insert(offset, stream_index);
                }
                _ => {}
            }
        }

        // pair up the index and segments (there should be an index per segment)
        let log_opts = opts.clone();
        let segments_len = segments.len();
        let mut closed = segments
            .into_iter()
            .enumerate()
            .map(move |(segment_idx, (i, segment))| {
                let is_last_segment = segment_idx == segments_len - 1;
                let index = match indexes.remove(&i) {
                    Some(index) => index,
                    None => {
                        let mut index = Index::new(
                            &log_opts.log_dir,
                            segment.starting_offset(),
                            log_opts.log_max_bytes,
                        )?;
                        // We always rehydrate the last segment, so no need to do it here
                        if !is_last_segment {
                            index.rehydrate(&segment, log_opts.message_max_bytes)?;
                            index.flush()?;
                        }
                        index
                    }
                };
                let stream_index = match stream_indexes.remove(&i) {
                    Some(stream_index) => stream_index,
                    None => {
                        let mut stream_index = StreamIndex::new(
                            &log_opts.log_dir,
                            segment.starting_offset(),
                            log_opts.log_max_entries,
                        )?;
                        // We always rehydrate the last segment, so no need to do it here
                        if !is_last_segment {
                            stream_index.rehydrate(&segment, &index, log_opts.message_max_bytes)?;
                            stream_index.flush()?;
                        }
                        stream_index
                    }
                };
                Ok((
                    i,
                    SegmentSet {
                        segment,
                        index,
                        stream_index,
                    },
                ))
            })
            .collect::<io::Result<BTreeMap<u64, SegmentSet>>>()?;

        // try to reuse the last index if it is not full. otherwise, open a new index
        // at the correct offset
        let last_entry = closed.keys().next_back().cloned();
        let mut active = match last_entry {
            Some(off) => {
                debug!("reusing index and segment starting at offset {}", off);
                closed.remove(&off).unwrap()
            }
            None => {
                debug!("starting new index and segment at offset 0");
                SegmentSet::new(&opts, 0)?
            }
        };

        // Rehydrate indexes
        active
            .index
            .rehydrate(&active.segment, log_opts.message_max_bytes)?;
        active.index.flush()?;
        active.stream_index.rehydrate(
            &active.segment,
            &active.index,
            log_opts.message_max_bytes,
        )?;
        active.stream_index.flush()?;

        // mark all closed indexes as readonly (indexes are not opened as readonly)
        for SegmentSet { index, .. } in closed.values_mut() {
            index.set_readonly()?;
        }

        Ok(FileSet {
            active,
            closed,
            opts,
        })
    }

    pub fn active_segment(&self) -> &Segment {
        &self.active.segment
    }

    pub fn active_index(&self) -> &Index {
        &self.active.index
    }

    pub fn active_stream_index(&self) -> &StreamIndex {
        &self.active.stream_index
    }

    pub fn active_segment_mut(&mut self) -> &mut Segment {
        &mut self.active.segment
    }

    pub fn active_index_mut(&mut self) -> &mut Index {
        &mut self.active.index
    }

    pub fn active_stream_index_mut(&mut self) -> &mut StreamIndex {
        &mut self.active.stream_index
    }

    pub fn find(&self, offset: u64) -> &SegmentSet {
        let active_seg_start_off = self.active.segment.starting_offset();
        if offset < active_seg_start_off {
            trace!(
                "Index is contained in the active index for offset {}",
                offset
            );
            if let Some(entry) = self.closed.range(..=offset).next_back().map(|p| p.1) {
                return entry;
            }
        }
        &self.active
    }

    pub fn roll_segment(&mut self) -> io::Result<()> {
        self.active.index.set_readonly()?; // Setting to read only flushes already
        self.active.stream_index.flush()?;
        self.active.segment.flush()?;

        let next_offset = self.active.index.next_offset();

        info!("Starting new segment and index at offset {}", next_offset);

        // set the segment and index to the new active index/seg
        let mut segment_set = SegmentSet::new(&self.opts, next_offset)?;
        swap(&mut segment_set, &mut self.active);
        self.closed
            .insert(segment_set.index.starting_offset(), segment_set);
        Ok(())
    }

    pub fn closed(&self) -> &BTreeMap<u64, SegmentSet> {
        &self.closed
    }

    pub fn remove_after(&mut self, offset: u64) -> Vec<SegmentSet> {
        if offset >= self.active.segment.starting_offset() {
            return vec![];
        }

        // find the midpoint
        //
        // E.g:
        //    offset = 6
        //    [0 5 10 15] => split key 5
        //
        // midpoint  is then used as the active index/segment pair
        let split_key = match self
            .closed
            .range(..=offset)
            .next_back()
            .map(|p| p.0)
            .cloned()
        {
            Some(key) => {
                trace!("File set split key for truncation {}", key);
                key
            }
            None => {
                warn!("Split key before offset {} found", offset);
                return vec![];
            }
        };

        // split off the range of close segment/index pairs including
        // the midpoint (which will become the new active index/segment)
        let mut after = self.closed.split_off(&split_key);

        let mut active = after.remove(&split_key).unwrap();
        trace!(
            "Setting active to segment starting {}",
            active.segment.starting_offset()
        );
        assert!(active.segment.starting_offset() <= offset);

        swap(&mut active, &mut self.active);

        let mut pairs = after.into_iter().map(|p| p.1).collect::<Vec<_>>();
        pairs.push(active);
        pairs
    }

    pub fn remove_before(&mut self, offset: u64) -> Vec<SegmentSet> {
        // split such that self.closed contains [..offset), suffix=[offset,...]
        let split_point = {
            match self
                .closed
                .range(..=offset)
                .next_back()
                .map(|(off, _)| off)
                .copied()
            {
                Some(off) => off,
                None => return vec![],
            }
        };

        let suffix = self.closed.split_off(&split_point);

        // put the suffix back into the closed segments
        let prefix = replace(&mut self.closed, suffix);
        prefix.into_values().collect()
    }

    pub fn log_options(&self) -> &LogOptions {
        &self.opts
    }

    /// First offset written. This may not be 0 due to removal of the start of
    /// the log
    pub fn min_offset(&self) -> Option<Offset> {
        if let Some(v) = self.closed.keys().next() {
            Some(*v)
        } else if !self.active.index.is_empty() {
            Some(self.active.index.starting_offset())
        } else {
            None
        }
    }
}
