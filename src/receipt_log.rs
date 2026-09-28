//! 本文を書き出さず、アプリへ渡した時刻と内容のfingerprintを記録する。
use crate::packet::fingerprint;
use serde::Serialize;
use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
};

pub struct ReceiptLog(Option<BufWriter<File>>);

#[derive(Serialize)]
pub struct Receipt<'a> {
    pub channel: u32,
    pub sequence: u64,
    pub received_us: u64,
    #[serde(skip)]
    pub payload: &'a [u8],
}

impl ReceiptLog {
    pub fn open(path: Option<PathBuf>) -> io::Result<Self> {
        Ok(Self(path.map(File::create).transpose()?.map(BufWriter::new)))
    }

    pub fn record(&mut self, receipt: Receipt<'_>) -> io::Result<()> {
        let Some(writer) = &mut self.0 else { return Ok(()) };
        let mut record = serde_json::to_value(&receipt).map_err(io::Error::other)?;
        record["bytes"] = receipt.payload.len().into();
        record["fingerprint"] = fingerprint(receipt.payload).into();
        serde_json::to_writer(&mut *writer, &record).map_err(io::Error::other)?;
        writeln!(writer)
    }

    pub fn finish(&mut self) -> io::Result<()> {
        if let Some(writer) = &mut self.0 {
            writer.flush()?;
        }
        Ok(())
    }
}
