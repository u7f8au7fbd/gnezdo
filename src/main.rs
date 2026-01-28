// main.rs - Gnezdo Ver 1.3
//
// Ver 1.2からの変更点:
// - URL重複除外機能追加（同一ページ内で同じURLが複数ある場合、上位優先で残す）
//
// Ver 1.1からの変更点:
// - 位置情報ポップアップ完全ブロック機能追加
//   - CSS強制非表示（ダイアログ/ライトボックス）
//   - Geolocation API完全無効化（JS側）
//   - permissions.query偽装（geolocationを常にdenied）
//   - 精密セレクタによる「後で」ボタン検出
//   - MutationObserver強化（属性変更も監視）
//   - 多重監視機構（RAF/イベント/定期チェック）
//   - 要素削除機能追加

mod core;
mod scrape;

use anyhow::Result;
use chrono::Local;
use core::config::load_config;
use core::paths::init_result_dir;
use core::runner::run_all_queries;
use core::utils::format_duration;

// ============================================================
// メイン
// ============================================================
fn main() -> Result<()> {
    let program_start = Local::now();
    println!("Gnezdo Ver 1.3 起動");
    println!("開始時刻: {}", program_start.format("%Y-%m-%d %H:%M:%S"));

    if cfg!(debug_assertions) {
        println!("モード: デバッグ（カレントディレクトリ基準）");
    } else {
        println!("モード: リリース（実行ファイル基準）");
    }

    // 設定読み込み
    let config = load_config();

    // 設定内容表示
    println!("\n--- 設定 ---");
    println!("  profile_dir: {}", config.profile_dir);
    println!("  chromium_path: {}", config.chromium_path);
    println!("  result_dir: {}", config.result_dir);
    println!("  max_pages: {}", config.max_pages);
    println!(
        "  max_consecutive_no_next: {}",
        config.max_consecutive_no_next
    );
    println!("  search_queries: {:?}", config.search_queries);
    println!("------------\n");

    let result_base = init_result_dir(&config, program_start)?;

    let mut manager = core::browser::BrowserManager::new(&config);
    manager.get_or_create()?;

    if let Err(e) = run_all_queries(&mut manager, program_start, &result_base, &config) {
        println!("致命的エラー: {}", e);
    }

    let program_end = Local::now();
    println!("\n========================================");
    println!("プログラム終了");
    println!("終了時刻: {}", program_end.format("%Y-%m-%d %H:%M:%S"));
    println!(
        "総実行時間: {}",
        format_duration(program_start, program_end)
    );
    println!("========================================");

    println!("\n終了。Enterで閉じる...");
    let _ = std::io::stdin().read_line(&mut String::new());

    Ok(())
}
