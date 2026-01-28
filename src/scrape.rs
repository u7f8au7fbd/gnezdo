use anyhow::Result;
use chrono::Local;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use crate::core::storage::save_json;
use std::path::Path;

// ============================================================
// HTML抽出 + 保存（このファイルで取得/保存を一元管理）
// ============================================================
const SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug)]
struct PageResult {
    schema_version: u32,
    query: String,
    page: u32,
    timestamp: String,
    result_count: usize,
    results: Vec<Value>,
}

fn build_page_result_from_html(query: &str, page_num: u32, html: &str) -> PageResult {
    // ===== ここだけ編集すれば取得/保存内容を変更できる =====
    // 取得対象を変える場合は、このブロックだけ修正してください。
    let document = Html::parse_document(html);
    let selector = Selector::parse(r#"a[jsname="UWckNb"]"#).unwrap();
    let title_selector = Selector::parse("h3").unwrap();
    let mut seen_urls: HashSet<String> = HashSet::new();
    let mut results: Vec<Value> = Vec::new();

    for element in document.select(&selector) {
        let url = element.value().attr("href").unwrap_or("").to_string();
        let title = element
            .select(&title_selector)
            .next()
            .map(|h3| h3.text().collect::<String>())
            .unwrap_or_default();

        // URL重複チェック（上位優先で残す）
        if !url.is_empty() && !title.is_empty() && !seen_urls.contains(&url) {
            seen_urls.insert(url.clone());
            results.push(json!({
                "title": title,
                "url": url
            }));
        }
    }

    PageResult {
        schema_version: SCHEMA_VERSION,
        query: query.to_string(),
        page: page_num,
        timestamp: Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        result_count: results.len(),
        results,
    }
}

pub fn build_and_save(
    query_dir: &Path,
    query: &str,
    page_num: u32,
    html: &str,
) -> Result<bool> {
    let page_result = build_page_result_from_html(query, page_num, html);
    if page_result.result_count == 0 {
        return Ok(false);
    }
    save_json(query_dir, page_num, &page_result)?;
    Ok(true)
}
