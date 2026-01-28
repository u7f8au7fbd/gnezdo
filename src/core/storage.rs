use anyhow::Result;
use std::fs;
use std::io::Write;
use std::path::Path;

pub fn save_json<T: serde::Serialize>(dir: &Path, page_num: u32, data: &T) -> Result<()> {
    let file_path = dir.join(format!("{}.json", page_num));
    let json = serde_json::to_string_pretty(data)?;
    let mut file = fs::File::create(&file_path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}
