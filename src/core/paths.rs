use crate::core::config::Config;
use anyhow::Result;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub fn get_base_path(relative: &str) -> PathBuf {
    if cfg!(debug_assertions) {
        let current_dir = env::current_dir().expect("カレントディレクトリ取得失敗");
        current_dir.join(relative)
    } else {
        let exe_path = env::current_exe().expect("実行ファイルパス取得失敗");
        exe_path.parent().unwrap().join(relative)
    }
}

pub fn init_profile_dir(config: &Config) -> Result<PathBuf> {
    let path = get_base_path(&config.profile_dir);
    fs::create_dir_all(&path)?;
    Ok(path)
}

pub fn clear_profile_dir(config: &Config) -> Result<()> {
    let path = get_base_path(&config.profile_dir);
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    Ok(())
}

pub fn init_result_dir(config: &Config, start_time: chrono::DateTime<chrono::Local>) -> Result<PathBuf> {
    let time_str = start_time.format("%Y-%m-%d-%H-%M-%S").to_string();
    let path = get_base_path(&config.result_dir).join(&time_str);
    fs::create_dir_all(&path)?;
    Ok(path)
}

pub fn init_query_result_dir(result_base: &Path, query: &str) -> Result<PathBuf> {
    let safe_query = query.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_");
    let path = result_base.join(&safe_query);
    fs::create_dir_all(&path)?;
    Ok(path)
}
