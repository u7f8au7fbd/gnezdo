use anyhow::Result;
use nanorand::{Rng, WyRand};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use headless_chrome::Tab;

// ============================================================
// 人間らしいスクロール
// ============================================================
pub fn human_scroll_to_bottom_medium(tab: &Arc<Tab>) -> Result<()> {
    let mut rng = WyRand::new();

    let mut mode_steps_remaining = 0;
    let mut current_mode: u8 = 0;

    loop {
        let scroll_info = tab.evaluate(
            "JSON.stringify({ scrollY: window.scrollY, innerHeight: window.innerHeight, scrollHeight: document.body.scrollHeight })",
            false,
        )?;

        let info: serde_json::Value =
            serde_json::from_str(scroll_info.value.unwrap().as_str().unwrap_or("{}"))
                .unwrap_or(serde_json::json!({}));

        let scroll_y = info["scrollY"].as_f64().unwrap_or(0.0);
        let inner_height = info["innerHeight"].as_f64().unwrap_or(800.0);
        let scroll_height = info["scrollHeight"].as_f64().unwrap_or(0.0);

        if scroll_y + inner_height >= scroll_height - 10.0 {
            break;
        }

        if mode_steps_remaining == 0 {
            current_mode = rng.generate_range(0_u8..=2);
            mode_steps_remaining = rng.generate_range(8_u32..=25);
        }
        mode_steps_remaining -= 1;

        let (scroll_amount, base_delay) = match current_mode {
            0 => (
                rng.generate_range(275_i32..=300),
                rng.generate_range(20_u64..=25),
            ),
            1 => (
                rng.generate_range(300_i32..=325),
                rng.generate_range(15_u64..=20),
            ),
            _ => (
                rng.generate_range(325_i32..=350),
                rng.generate_range(10_u64..=15),
            ),
        };

        tab.evaluate(
            &format!(
                "window.scrollBy({{ top: {}, behavior: 'auto' }})",
                scroll_amount
            ),
            false,
        )?;

        thread::sleep(Duration::from_millis(base_delay));

        if rng.generate_range(0_u32..100) < 30 {
            let pause = rng.generate_range(240..=720);
            human_pause_with_keepalive(tab, pause)?;
        }

        if rng.generate_range(0_u32..100) < 20 {
            let pause = rng.generate_range(1200..=2400);
            println!("  ...{}ms 閲覧中", pause);
            human_pause_with_keepalive(tab, pause)?;
        }

        if rng.generate_range(0_u32..100) < 10 {
            let back_amount = rng.generate_range(38_i32..=112);
            tab.evaluate(
                &format!(
                    "window.scrollBy({{ top: -{}, behavior: 'auto' }})",
                    back_amount
                ),
                false,
            )?;
            let pause = rng.generate_range(360..=960);
            human_pause_with_keepalive(tab, pause)?;
        }
    }

    Ok(())
}

// ============================================================
// Keep-Alive付き停止
// ============================================================
pub fn human_pause_with_keepalive(tab: &Arc<Tab>, total_ms: u64) -> Result<()> {
    let interval = 400;
    let mut elapsed = 0;

    while elapsed < total_ms {
        let sleep_time = (total_ms - elapsed).min(interval);
        thread::sleep(Duration::from_millis(sleep_time));
        elapsed += sleep_time;
        tab.evaluate("1", false)?;
    }

    Ok(())
}

// ============================================================
// 人間らしいタイピング
// ============================================================
pub fn human_type_medium(tab: &Arc<Tab>, text: &str) -> Result<()> {
    let mut rng = WyRand::new();

    for c in text.chars() {
        tab.send_character(&c.to_string())?;
        let delay = rng.generate_range(75_u64..=300);
        thread::sleep(Duration::from_millis(delay));
    }

    Ok(())
}
