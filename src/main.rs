use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self},
    io::{BufRead, BufReader},
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

fn main() -> Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();

    let folder = &args[1];
    let file_paths: Vec<DirEntry> = WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
        .collect();

    for file in file_paths {
        let decoder = {
            let file = fs::File::open(file.path()).unwrap();
            zstd::Decoder::new(file).unwrap()
        };
        let reader = BufReader::new(decoder);
        let encoder = {
            let file = fs::File::create("hplt.wet.zst").unwrap();
            zstd::Encoder::new(file, 0).unwrap()
        };
        let mut warc_file = WarcWriter::new(encoder.auto_finish());

        // let mut line_count = 0;
        for line in reader.lines() {
            let doc: HpltJson = serde_json::from_str(&line.unwrap()).unwrap();
            // if line_count == 100000 {
            //     println!("100000 lines written");
            //     break;
            // }
            let builder = RecordBuilder::default()
                .body(doc.text.into_bytes())
                .version("1.0".to_owned())
                .header(WarcHeader::TargetURI, doc.url.into_bytes())
                .header(WarcHeader::ContentType, "text/plain".as_bytes())
                .header(WarcHeader::WarcType, "conversion".as_bytes());

            let record = builder.build().unwrap();

            warc_file.write(&record)?;
            // line_count += 1;
        }
    }
    Ok(())
}
