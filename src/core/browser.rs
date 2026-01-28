use crate::core::config::Config;
use crate::core::paths::{clear_profile_dir, get_base_path, init_profile_dir};
use anyhow::Result;
use headless_chrome::{Browser, LaunchOptions, Tab};
use std::ffi::OsStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct BrowserManager<'a> {
    browser: Option<Browser>,
    config: &'a Config,
}

impl<'a> BrowserManager<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self {
            browser: None,
            config,
        }
    }

    pub fn get_or_create(&mut self) -> Result<&Browser> {
        if self.browser.is_none() {
            self.browser = Some(launch_browser(self.config)?);
        }
        Ok(self.browser.as_ref().unwrap())
    }

    pub fn restart(&mut self) -> Result<&Browser> {
        println!("ブラウザを再起動中（profileリセット）...");
        self.browser = None;
        thread::sleep(Duration::from_millis(2000));
        self.browser = Some(launch_browser(self.config)?);
        Ok(self.browser.as_ref().unwrap())
    }
}

// ============================================================
// ブラウザ起動
// ============================================================
pub fn launch_browser(config: &Config) -> Result<Browser> {
    println!("profile を強制リセット中...");
    let _ = clear_profile_dir(config);
    println!("profile 削除完了。新規作成中...");
    let user_data_dir = init_profile_dir(config)?;
    println!("新規 profile: {:?}", user_data_dir);

    let chromium_path = get_base_path(&config.chromium_path);
    println!("Chromium: {:?}", chromium_path);

    let args: Vec<&OsStr> = vec![
        OsStr::new("--no-sandbox"),
        OsStr::new("--disable-setuid-sandbox"),
        OsStr::new("--disable-infobars"),
        OsStr::new("--no-first-run"),
        OsStr::new("--no-default-browser-check"),
        OsStr::new("--window-size=1920,1080"),
        OsStr::new("--start-maximized"),
        OsStr::new("--disable-blink-features=AutomationControlled"),
        OsStr::new("--webrtc-ip-handling-policy=default_public_interface_only"),
        OsStr::new("--force-webrtc-ip-handling-policy"),
        OsStr::new(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.7499.110 Safari/537.36",
        ),
        OsStr::new("--lang=ja-JP,ja"),
        OsStr::new("--use-angle=d3d11"),
        OsStr::new("--enable-gpu-rasterization"),
        OsStr::new("--enable-zero-copy"),
        OsStr::new("--ignore-gpu-blocklist"),
        OsStr::new("--disable-dev-shm-usage"),
        OsStr::new("--disable-geolocation"),
        OsStr::new("--disable-notifications"),
        OsStr::new("--disable-popup-blocking"),
    ];

    let ignore_default_args: Vec<&OsStr> = vec![OsStr::new("--enable-automation")];

    let browser = Browser::new(LaunchOptions {
        headless: false,
        window_size: Some((1920, 1080)),
        sandbox: false,
        enable_gpu: true,
        user_data_dir: Some(user_data_dir),
        path: Some(chromium_path),
        args,
        ignore_default_args,
        disable_default_args: false,
        idle_browser_timeout: Duration::from_secs(600),
        ..Default::default()
    })?;

    Ok(browser)
}

// ============================================================
// アクティブタブ取得
// ============================================================
pub fn get_active_tab(manager: &mut BrowserManager) -> Result<Arc<Tab>> {
    let browser = manager.get_or_create()?;
    thread::sleep(Duration::from_millis(500));

    let tab = {
        let tabs = browser.get_tabs().lock().unwrap();
        let first_tab = tabs.first().cloned();
        for tab in tabs.iter().skip(1) {
            let _ = tab.close(false);
        }
        first_tab
    };

    match tab {
        Some(t) => Ok(t),
        None => {
            let browser = manager.get_or_create()?;
            Ok(browser.new_tab()?)
        }
    }
}
