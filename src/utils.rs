use anyhow::anyhow;
use chrono::NaiveDate;
use databento::dbn::{decode::AsyncDbnDecoder, MboMsg, PitSymbolMap};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use time::macros::format_description;
use time::Date;

// Extract time::Date from filename
pub fn extract_date_from_filename(file_path: &Path) -> anyhow::Result<Date> {
    let re = regex::Regex::new(r"(\d{8})").unwrap();
    if let Some(caps) = re.captures(file_path.to_str().unwrap()) {
        let date_str = caps.get(1).unwrap().as_str();
        let format = format_description!("[year][month][day]");
        Ok(Date::parse(date_str, format)?)
    } else {
        Err(anyhow!("Invalid filename"))
    }
}

// List files sorted by date
pub fn list_files_sorted_by_date(dir_path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut file_paths: Vec<(NaiveDate, PathBuf)> = vec![];

    for entry in fs::read_dir(dir_path)? {
        let path = entry?.path();
        if path.is_file() && path.extension().unwrap() == "zst" {
            // Check if it's a file
            let time_date = extract_date_from_filename(&path)?;
            let date = convert_time_date_to_chrono_naive_date(time_date).unwrap();
            file_paths.push((date, path));
        }
    }

    // Sort by date
    file_paths.sort_by_key(|k| k.0);
    Ok(file_paths.into_iter().map(|(_, path)| path).collect())
}

// Convert time::Date to chrono::NaiveDate
pub fn convert_time_date_to_chrono_naive_date(time_date: Date) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(
        time_date.year(),
        time_date.month() as u32,
        time_date.day() as u32,
    )
}

// Get symbols for given file path and date
pub async fn get_symbols_for_file_path_and_date(
    file_path: &PathBuf,
    time_date: Date,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut decoder = AsyncDbnDecoder::from_zstd_file(file_path).await?;
    let metadata = decoder.metadata().clone();
    let pit_symbol_map = PitSymbolMap::from_metadata(&metadata, time_date);
    if pit_symbol_map.is_err() {
        return Ok(vec![]);
    }
    let pit_symbol_map = pit_symbol_map.unwrap();
    let mut symbols: Vec<String> = vec![];
    while let Some(msg) = decoder.decode_record::<MboMsg>().await? {
        let msg_date = msg
            .hd
            .ts_event()
            .unwrap()
            .to_offset(time::UtcOffset::UTC)
            .date();
        if msg_date == time_date {
            if let Some(key) = pit_symbol_map.get(msg.hd.instrument_id) {
                symbols.push(key.clone());
            }
        }
    }
    Ok(symbols)
}
