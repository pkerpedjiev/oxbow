use std::fs::File;
use std::io; 
// use std::io::{self, BufReader, Read, Seek};
use std::io::{Read, Seek};
// use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, GenericStringBuilder, Int32Builder
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use noodles::core::Region;
use noodles::csi::io::{IndexedRecord, IndexedReader};
// use noodles::csi::io::IndexedRecords;
// use noodles::csi::io::Query;
use noodles::{bgzf, csi, tabix};

use crate::batch_builder::{write_ipc_err, BatchBuilder};


fn read_magic(read: &mut dyn Read) -> io::Result<[u8; 4]> {
    let mut magic = [0; 4];
    let mut bgzf_file = bgzf::Reader::new(read);
    bgzf_file.read_exact(&mut magic)?;
    Ok(magic)
}

pub fn index_from_reader<R>(mut read: R) -> io::Result<csi::Index>
where
    R: Read + Seek,
{
    let magic = read_magic(&mut read)?;
    read.seek(io::SeekFrom::Start(0))?;
    if magic == b"TBI\x01" as &[u8] {
        let mut tbi_reader = tabix::Reader::new(read);
        tbi_reader.read_index()
    } else {
        let mut csi_reader = csi::Reader::new(read);
        csi_reader.read_index()
    }
}

pub struct TabixReader<R> {
    reader: csi::io::IndexedReader<bgzf::Reader<R>>,
}

impl TabixReader<File> {
    pub fn new_from_path(path: &str) -> io::Result<Self> {
        let reader = tabix::io::indexed_reader::Builder::default().build_from_path(path)?;
        Ok(Self { reader })
    }
}

impl<R: Read + Seek> TabixReader<R> {
    pub fn new(read: R, index: csi::Index) -> std::io::Result<Self> {
        let reader = IndexedReader::new(read, index);
        Ok(Self { reader })
    }

    pub fn records_to_ipc(&mut self, region: Option<&str>) -> Result<Vec<u8>, ArrowError> {
        let batch_builder = TabixBatchBuilder::new()?;
        if let Some(region) = region {
            let region: Region = region.parse().unwrap();
            let query = self
                .reader
                .query(&region)
                .map(|record| {
                    record.map(|record| {
                        let record = record.unwrap();
                        let chrom = record.indexed_reference_sequence_name().to_string();
                        let start = record.indexed_start_position().get() as i32;
                        let end = record.indexed_end_position().get() as i32;
                        let raw = record.as_ref().to_string();
                        Ok(TabixRecord { chrom, start, end, raw })
                    })
                })
                .map_err(|e| ArrowError::ExternalError(e.into()))?;
                // .map(|i| i.map_err(|e| ArrowError::ExternalError(e.into())));
            return write_ipc_err(query, batch_builder);
        } else {
            // throw an error for now
            return Err(ArrowError::ExternalError("No region specified".into()));
        }
    }
}

pub struct TabixRecord {
    chrom: String,
    start: i32,
    end: i32,
    raw: String,
}

struct TabixBatchBuilder {
    chrom: GenericStringBuilder<i32>,
    start: Int32Builder,
    end: Int32Builder,
    raw: GenericStringBuilder<i32>,
}

impl TabixBatchBuilder {
    fn new() -> Result<Self, ArrowError> {
        Ok(Self {
            chrom: GenericStringBuilder::new(),
            start: Int32Builder::new(),
            end: Int32Builder::new(),
            raw: GenericStringBuilder::new(),
        })
    }
}

impl BatchBuilder for TabixBatchBuilder {
    
    type Record<'a> = &'a TabixRecord;

    fn push(&mut self, record: Self::Record<'_>) {
        self.chrom.append_value(&record.chrom);
        self.start.append_value(record.start);
        self.end.append_value(record.end);
        self.raw.append_value(&record.raw);
    }

    fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_from_iter(vec![
            ("chrom", Arc::new(self.chrom.finish()) as ArrayRef),
            ("start", Arc::new(self.start.finish()) as ArrayRef),
            ("end", Arc::new(self.end.finish()) as ArrayRef),
            ("raw", Arc::new(self.raw.finish()) as ArrayRef),
        ])
    }
}
