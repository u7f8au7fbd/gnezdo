use crate::core::browser::{get_active_tab, BrowserManager};
use crate::core::config::Config;
use crate::core::human::{human_pause_with_keepalive, human_scroll_to_bottom_medium, human_type_medium};
use crate::core::paths::init_query_result_dir;
use crate::core::stealth::{inject_stealth_scripts, setup_stealth_cdp};
use crate::core::utils::format_duration;
use crate::scrape::build_and_save;
use anyhow::Result;
use chrono::{DateTime, Local};
use nanorand::{Rng, WyRand};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============================================================
// 全クエリ実行
// ============================================================
pub fn run_all_queries(
    manager: &mut BrowserManager,
    program_start: DateTime<Local>,
    result_base: &Path,
    config: &Config,
) -> Result<()> {
    let mut rng = WyRand::new();
    let mut query_index = 0;
    let mut retry_count = 0;
    let mut consecutive_no_next = 0;
    const MAX_RETRIES: u32 = 3;

    let queries = &config.search_queries;

    while query_index < queries.len() {
        let query = &queries[query_index];
        let query_start = Local::now();

        println!("\n========================================");
        println!("検索 {}/{}: 「{}」", query_index + 1, queries.len(), query);
        println!("開始: {}", query_start.format("%H:%M:%S"));
        println!("========================================");

        let query_dir = init_query_result_dir(result_base, query)?;

        let tab = match get_active_tab(manager) {
            Ok(t) => t,
            Err(e) => {
                println!("タブ取得エラー: {}。ブラウザ再起動。", e);
                if let Err(restart_err) = manager.restart() {
                    println!("再起動失敗: {}。スキップ。", restart_err);
                    query_index += 1;
                    retry_count = 0;
                    continue;
                }
                match get_active_tab(manager) {
                    Ok(t) => t,
                    Err(e) => {
                        println!("再起動後もタブ取得失敗: {}。スキップ。", e);
                        query_index += 1;
                        retry_count = 0;
                        continue;
                    }
                }
            }
        };

        let _ = setup_stealth_cdp(&tab);
        let _ = inject_stealth_scripts(&tab);

        match execute_single_query(&tab, query, &query_dir, &mut consecutive_no_next, config) {
            Ok(_) => {
                let query_end = Local::now();
                let total_now = Local::now();

                println!("----------------------------------------");
                println!("「{}」完了", query);
                println!(
                    "  クエリ所要時間: {}",
                    format_duration(query_start, query_end)
                );
                println!(
                    "  累計経過時間: {}",
                    format_duration(program_start, total_now)
                );
                println!("----------------------------------------");

                query_index += 1;
                retry_count = 0;

                if query_index < queries.len() {
                    let rest = rng.generate_range(3600..=7200);
                    println!("次のクエリまで {}ms 休憩...", rest);
                    thread::sleep(Duration::from_millis(rest));

                    println!("再起動して profile リセット...");
                    let _ = manager.restart();
                }
            }
            Err(e) => {
                retry_count += 1;
                println!(
                    "検索エラー: {}。リトライ {}/{}",
                    e, retry_count, MAX_RETRIES
                );

                if retry_count >= MAX_RETRIES {
                    println!("リトライ上限。次のクエリへスキップ。");
                    query_index += 1;
                    retry_count = 0;
                } else {
                    println!("ブラウザ再起動して profile リセット...");
                    let _ = manager.restart();
                }
                continue;
            }
        }
    }

    println!("\n========================================");
    println!("全クエリ巡回完了！");
    println!("========================================");

    Ok(())
}

// ============================================================
// 単一クエリ実行
// ============================================================
fn execute_single_query(
    tab: &Arc<headless_chrome::Tab>,
    query: &str,
    query_dir: &Path,
    consecutive_no_next: &mut u32,
    config: &Config,
) -> Result<()> {
    // ===== 初期化 =====
    tab.navigate_to("about:blank")?;
    thread::sleep(Duration::from_millis(300));
    tab.evaluate("1", false)?;

    // ===== Googleトップ =====
    tab.navigate_to("https://www.google.com")?;
    tab.wait_until_navigated()?;
    human_pause_with_keepalive(tab, 960)?;

    // ===== 検索ボックス =====
    let search_box = tab.wait_for_element("textarea[name='q']")?;
    search_box.click()?;
    human_type_medium(tab, query)?;
    thread::sleep(Duration::from_millis(450));

    tab.press_key("Enter")?;
    tab.wait_until_navigated()?;
    human_pause_with_keepalive(tab, 600)?;

    // ===== 検索結果ページループ =====
    for page in 0..config.max_pages {
        let page_num = page + 1;
        println!("  ページ {}/{}", page_num, config.max_pages);

        tab.evaluate("1", false)?;
        human_pause_with_keepalive(tab, 960)?;

        let html = tab.get_content()?;
        let saved = build_and_save(query_dir, query, page_num, &html)?;
        if !saved {
            println!("  警告: 検索結果が見つかりませんでした");
        }

        human_scroll_to_bottom_medium(tab)?;
        human_pause_with_keepalive(tab, 750)?;

        if page_num >= config.max_pages {
            println!("  最終ページ到達。");
            break;
        }

        match tab.wait_for_element_with_custom_timeout("#pnnext", Duration::from_secs(3)) {
            Ok(next_button) => {
                *consecutive_no_next = 0;
                next_button.click()?;
                tab.wait_until_navigated()?;
                human_pause_with_keepalive(tab, 480)?;
            }
            Err(_) => {
                *consecutive_no_next += 1;
                println!(
                    "  「次へ」が見つかりません（連続{}回目）",
                    consecutive_no_next
                );

                if *consecutive_no_next >= config.max_consecutive_no_next {
                    println!("\n========================================");
                    println!(
                        "警告: 「次へ」が連続{}回見つかりませんでした",
                        consecutive_no_next
                    );
                    println!("Bot検出の可能性があります。");
                    println!("Enterを押すと続行します...");
                    println!("========================================");
                    let _ = std::io::stdin().read_line(&mut String::new());
                    *consecutive_no_next = 0;
                }
                break;
            }
        }
    }

    Ok(())
}
