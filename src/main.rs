use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self},
    io::{BufRead, BufReader},
    path::Path,
};

use walkdir::{DirEntry, WalkDir};
use warc::{RecordBuilder, WarcHeader, WarcWriter};

#[derive(Serialize, Deserialize, Debug)]
struct HpltJson {
    id: Option<String>,
    document_lang: String,
    scores: Vec<f32>,
    langs: Vec<String>,
    text: String,
    url: String,
    collection: String,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let folder = &args[1];
    let dst = &args[2];
    let file_paths: Vec<DirEntry> = WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
        .collect();

    file_paths.into_par_iter().for_each(|file| {
        let path = file.path();
        let file_stem = path.file_stem().unwrap();
        let file_name = Path::new(file_stem).file_stem().unwrap().to_str().unwrap();
        let decoder = {
            let file = fs::File::open(path).unwrap();
            zstd::Decoder::new(file).unwrap()
        };
        let reader = BufReader::new(decoder);
        let encoder = {
            let file = fs::File::create(format!("{}/{}.{}", dst, file_name, "wet.zst")).unwrap();
            zstd::Encoder::new(file, 0).unwrap()
        };
        let mut warc_file = WarcWriter::new(encoder.auto_finish());

        for line in reader.lines() {
            let doc: HpltJson = serde_json::from_str(&line.unwrap()).unwrap();
            let builder = RecordBuilder::default()
                .body(doc.text.into_bytes())
                .version("1.0".to_owned())
                .header(WarcHeader::TargetURI, doc.url.into_bytes())
                .header(WarcHeader::ContentType, "text/plain".as_bytes())
                .header(WarcHeader::WarcType, "conversion".as_bytes());

            let record = builder.build().unwrap();

            warc_file.write(&record).expect("Falied to write record");
        }
    });
}
