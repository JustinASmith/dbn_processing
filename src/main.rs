use chrono::NaiveDate;
use databento::dbn::{
    decode::AsyncDbnDecoder, encode::dbn::AsyncEncoder as AsyncDbnEncoder, MboMsg, Metadata,
    PitSymbolMap,
};
use dbn_processing::utils::{
    convert_time_date_to_chrono_naive_date, extract_date_from_filename, list_files_sorted_by_date,
};
use std::collections::HashMap;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use time::Date;
use tokio::fs::OpenOptions;

// Process file for date
pub async fn process_file_for_date(
    file_path: &PathBuf,
    time_date: Date,
    instrument_data: &mut HashMap<String, Vec<MboMsg>>,
) -> Result<(bool, Option<Metadata>), Box<dyn std::error::Error>> {
    let mut decoder = AsyncDbnDecoder::from_zstd_file(file_path).await?;
    let metadata = decoder.metadata().clone();

    let pit_symbol_map = PitSymbolMap::from_metadata(&metadata, time_date);
    if pit_symbol_map.is_err() {
        return Ok((false, Some(metadata)));
    }
    let pit_symbol_map = pit_symbol_map.unwrap();

    while let Some(msg) = decoder.decode_record::<MboMsg>().await? {
        let msg_date = msg
            .hd
            .ts_event()
            .unwrap()
            .to_offset(time::UtcOffset::UTC)
            .date();
        if msg_date == time_date {
            if let Some(key) = pit_symbol_map.get(msg.hd.instrument_id) {
                instrument_data
                    .entry(key.clone())
                    .or_default()
                    .push(msg.clone());
            }
        }
    }

    Ok((true, Some(metadata)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut processed_dates: HashSet<NaiveDate> = HashSet::new();
    let dir_path = Path::new("/media/jbug/HDD/HistoricalData/MBO/ES-NQ");
    let files = list_files_sorted_by_date(dir_path)?;

    let mut current_index = 0;
    while current_index < files.len() {
        let file_path = &files[current_index];
        let time_date = extract_date_from_filename(file_path)?;
        let date = convert_time_date_to_chrono_naive_date(time_date).unwrap();

        if !processed_dates.contains(&date) {
            let mut instrument_data: HashMap<String, Vec<MboMsg>> = HashMap::new();
            let mut metadata: Option<Metadata> = None;

            // Process files for the current date
            while current_index < files.len() {
                let file_path = &files[current_index];
                println!("Processing file: {}", file_path.to_str().unwrap());
                let (has_data_for_date, decoder_metadata) =
                    process_file_for_date(file_path, time_date, &mut instrument_data).await?;
                if has_data_for_date {
                    current_index += 1;
                    metadata = decoder_metadata;
                } else {
                    break; // Break if the current file does not contain data for the date
                }
            }

            // For each symbol in the aggregated data save to a file
            for (symbol, msgs) in instrument_data.iter_mut() {
                if let Some(meta) = metadata.as_ref() {
                    let sub_folder = match symbol {
                        s if s.contains("ES") => "ES",
                        s if s.contains("NQ") => "NQ",
                        _ => "Other",
                    };
                    // Save dbn data to file
                    let path = format!(
                        "/media/jbug/HDD/HistoricalData/MBO/ES-NQ/{}/{}.dbn.zst",
                        sub_folder, symbol
                    );
                    let mut file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .append(true)
                        .open(path)
                        .await?;
                    let mut encoder = AsyncDbnEncoder::new(&mut file, meta).await?;
                    for msg in msgs.iter() {
                        //aggregated_data.messages {
                        encoder.encode_record(msg).await?;
                    }
                    encoder.flush().await?;
                }
            }

            processed_dates.insert(date);
            println!("Processed date: {}", date);
        } else {
            current_index += 1; // Move to the next file if the date has already been processed
        }
    }

    Ok(())
}
