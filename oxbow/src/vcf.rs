use arrow::array::{
    ArrayRef, Float32Builder, GenericStringBuilder, Int32Builder,
    StringArray, StringDictionaryBuilder,
};
use arrow::{
    datatypes::Int32Type, error::ArrowError, record_batch::RecordBatch,
};
use noodles::core::Region;
use noodles::{tabix, vcf};
use std::sync::Arc;

use crate::batch_builder::{write_ipc, BatchBuilder};

type BufferedReader = std::io::BufReader<std::fs::File>;

/// A VCF reader.
pub struct VcfReader {
    reader: vcf::IndexedReader<BufferedReader>,
    header: vcf::Header,
}

impl VcfReader {
    /// Creates a VCF Reader.
    pub fn new(path: &str) -> std::io::Result<Self> {
        let index = tabix::read(format!("{}.tbi", path))?;
        let file = std::fs::File::open(path)?;
        let bufreader = std::io::BufReader::with_capacity(1024 * 1024, file);
        let mut reader = vcf::indexed_reader::Builder::default()
            .set_index(index)
            .build_from_reader(bufreader)?;
        let header = reader.read_header()?;
        Ok(Self { reader, header })
    }

    /// Returns the records in the given region as Apache Arrow IPC.
    ///
    /// If the region is `None`, all records are returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use oxbow::vcf::VcfReader;
    ///
    /// let mut reader = VcfReader::new("sample.vcf.gz").unwrap();
    /// let ipc = reader.records_to_ipc(Some("sq0:1-1000")).unwrap();
    /// ```
    pub fn records_to_ipc(&mut self, region: Option<&str>) -> Result<Vec<u8>, ArrowError> {
        let mut batch_builder = VcfBatchBuilder::new(1024, &self.header)?;
        if let Some(region) = region {
            let region: Region = region.parse().unwrap();
            let query = self
                .reader
                .query(&self.header, &region)
                .unwrap()
                .map(|r| r.unwrap());
            todo!();
        }
        let mut buf = String::new();
        loop {
            buf.clear();
            match read_line(self.reader.get_mut(), &mut buf)? {
                0 => break,
                _ => {
                    batch_builder.push(&buf);
                }
            }
        }

        let batch = batch_builder.finish()?;
        let mut writer = arrow::ipc::writer::FileWriter::try_new(Vec::new(), &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
        writer.into_inner()
    }
}

// Reads all bytes until a line feed ('\n') or EOF is reached.
//
// The buffer will not include the trailing newline ('\n' or '\r\n').
fn read_line<R>(reader: &mut R, buf: &mut String) -> std::io::Result<usize>
where
    R: std::io::BufRead,
{
    const LINE_FEED: char = '\n';
    const CARRIAGE_RETURN: char = '\r';

    match reader.read_line(buf) {
        Ok(0) => Ok(0),
        Ok(n) => {
            if buf.ends_with(LINE_FEED) {
                buf.pop();
                if buf.ends_with(CARRIAGE_RETURN) {
                    buf.pop();
                }
            }
            Ok(n)
        }
        Err(e) => Err(e),
    }
}

struct VcfBatchBuilder {
    chrom: StringDictionaryBuilder<Int32Type>,
    pos: Int32Builder,
    id: GenericStringBuilder<i32>,
    ref_: GenericStringBuilder<i32>,
    alt: GenericStringBuilder<i32>,
    qual: Float32Builder,
    filter: GenericStringBuilder<i32>,
    info: GenericStringBuilder<i32>,
    format: GenericStringBuilder<i32>,
}

impl VcfBatchBuilder {
    pub fn new(capacity: usize, header: &vcf::Header) -> Result<Self, ArrowError> {
        let categories = StringArray::from(
            header
                .contigs()
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<_>>(),
        );
        Ok(Self {
            chrom: StringDictionaryBuilder::<Int32Type>::new_with_dictionary(
                capacity,
                &categories,
            )?,
            pos: Int32Builder::with_capacity(capacity),
            id: GenericStringBuilder::<i32>::new(),
            ref_: GenericStringBuilder::<i32>::new(),
            alt: GenericStringBuilder::<i32>::new(),
            qual: Float32Builder::with_capacity(capacity),
            filter: GenericStringBuilder::<i32>::new(),
            info: GenericStringBuilder::<i32>::new(),
            format: GenericStringBuilder::<i32>::new(),
        })
    }
}

impl BatchBuilder for VcfBatchBuilder {
    type Record = String;

    fn push(&mut self, record: &Self::Record) {
        let mut fields = record.split('\t');
        self.chrom.append_option(fields.next());
        self.pos.append_option(fields.next().map(|x| x.parse().unwrap()));
        self.id.append_option(fields.next());
        self.ref_.append_option(fields.next());
        self.alt.append_option(fields.next());
        self.qual.append_option(fields.next().map(|x| x.parse().unwrap()));
        self.filter.append_option(fields.next());
        self.info.append_option(fields.next());
        self.format.append_option(fields.next());
    }

    fn finish(mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_from_iter(vec![
            // spec
            ("chrom", Arc::new(self.chrom.finish()) as ArrayRef),
            ("pos", Arc::new(self.pos.finish()) as ArrayRef),
            ("id", Arc::new(self.id.finish()) as ArrayRef),
            ("ref", Arc::new(self.ref_.finish()) as ArrayRef),
            ("alt", Arc::new(self.alt.finish()) as ArrayRef),
            ("qual", Arc::new(self.qual.finish()) as ArrayRef),
            ("filter", Arc::new(self.filter.finish()) as ArrayRef),
            ("info", Arc::new(self.info.finish()) as ArrayRef),
            ("format", Arc::new(self.format.finish()) as ArrayRef),
        ])
    }
}
