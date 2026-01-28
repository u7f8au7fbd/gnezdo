use crate::core::paths::get_base_path;
use serde::Deserialize;
use std::fs;

// ============================================================
// 設定構造体（Config.toml用）
// ============================================================
#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default = "default_profile_dir")]
    pub profile_dir: String,

    #[serde(default = "default_chromium_path")]
    pub chromium_path: String,

    #[serde(default = "default_result_dir")]
    pub result_dir: String,

    #[serde(default = "default_max_pages")]
    pub max_pages: u32,

    #[serde(default = "default_max_consecutive_no_next")]
    pub max_consecutive_no_next: u32,

    #[serde(default = "default_search_queries")]
    pub search_queries: Vec<String>,
}

fn default_profile_dir() -> String {
    "chromium/profile".to_string()
}
fn default_chromium_path() -> String {
    "chromium/chrome.exe".to_string()
}
fn default_result_dir() -> String {
    "result".to_string()
}
fn default_max_pages() -> u32 {
    10
}
fn default_max_consecutive_no_next() -> u32 {
    2
}
fn default_search_queries() -> Vec<String> {
    vec!["1".to_string(), "2".to_string(), "3".to_string()]
}

impl Default for Config {
    fn default() -> Self {
        Self {
            profile_dir: default_profile_dir(),
            chromium_path: default_chromium_path(),
            result_dir: default_result_dir(),
            max_pages: default_max_pages(),
            max_consecutive_no_next: default_max_consecutive_no_next(),
            search_queries: default_search_queries(),
        }
    }
}

pub fn load_config() -> Config {
    let config_path = get_base_path("Config.toml");
    if config_path.exists() {
        println!("設定ファイル読み込み: {:?}", config_path);
        if let Ok(content) = fs::read_to_string(&config_path)
            && let Ok(cfg) = toml::from_str(&content)
        {
            println!("設定ファイル読み込み成功");
            return cfg;
        }
    }
    println!("設定ファイル読み込み失敗。デフォルト使用。");
    Config::default()
}
