use std::fs::File;
use std::io; 
// use std::io::{self, BufReader, Read, Seek};
// use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, GenericStringBuilder, Int32Builder
};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use noodles::core::Region;
use noodles::csi::io::IndexedRecord;
use noodles::{bgzf, csi, tabix};

use crate::batch_builder::{write_ipc_err, BatchBuilder};

pub struct TabixReader {
    reader: csi::io::IndexedReader<bgzf::Reader<File>>,
}

impl TabixReader {
    pub fn new_from_path(path: &str) -> io::Result<Self> {
        let mut reader = tabix::io::indexed_reader::Builder::default().build_from_path(path)?;
        Ok(Self { reader })
    }

    pub fn records_to_ipc(&mut self, region: Option<&str>) -> Result<Vec<u8>, ArrowError> {
        let mut batch_builder = TabixBatchBuilder::new()?;
        if let Some(region) = region {
            let region: Region = region.parse().unwrap();
            let query = self
                .reader
                .query(&region)
                .map_err(|e| ArrowError::ExternalError(e.into()))?
                .map(|i| i.map_err(|e| ArrowError::ExternalError(e.into())));
            return write_ipc_err(query, batch_builder);
        } else {
            // throw an error for now
            return Err(ArrowError::ExternalError("No region specified".into()));
        }
    }
}

struct TabixBatchBuilder {
    chrom: GenericStringBuilder<i32>,
    start: Int32Builder,
    end: Int32Builder,
    rest: GenericStringBuilder<i32>,
}

impl TabixBatchBuilder {
    fn new() -> Result<Self, ArrowError> {
        Ok(Self {
            chrom: GenericStringBuilder::new(),
            start: Int32Builder::new(),
            end: Int32Builder::new(),
            rest: GenericStringBuilder::new(),
        })
    }
}

impl BatchBuilder for TabixBatchBuilder {
    type Record<'a> = &'a csi::io::indexed_records::record::Record;
    // type Record<'a> = &'a dyn csi::io::IndexedRecord;

    fn push(&mut self, record: Self::Record<'_>) {
        self.chrom.append_value(record.indexed_reference_sequence_name());
        self.start.append_value(record.indexed_start_position().get() as i32);
        self.end.append_value(record.indexed_end_position().get() as i32);
        // self.rest.append_value(record.as_ref());
    }

    fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_from_iter(vec![
            ("chrom", Arc::new(self.chrom.finish()) as ArrayRef),
            ("start", Arc::new(self.start.finish()) as ArrayRef),
            ("end", Arc::new(self.end.finish()) as ArrayRef),
            ("rest", Arc::new(self.rest.finish()) as ArrayRef),
        ])
    }
}




// fn main() -> Result<(), Box<dyn std::error::Error>>{

//     let src = "/Users/nezar/local/devel/oxbow_project/oxbow/fixtures/ALL.chrY.phase3_integrated_v1a.20130502.genotypes.vcf.gz";
//     let region = "Y:2657239-2700000".parse()?;
    
//     let mut reader = tabix::io::indexed_reader::Builder::default().build_from_path(src)?;
//     let query = reader.query(&region)?;
    
//     for result in query {
//         let record = result?;
//         println!("{:?}", record.as_ref());
//     };

//     Ok(())
// }