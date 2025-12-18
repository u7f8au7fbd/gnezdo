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

use anyhow::Result;
use chrono::{DateTime, Local};
use headless_chrome::{Browser, LaunchOptions, Tab};
use nanorand::{Rng, WyRand};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ============================================================
// 設定構造体（Config.toml用）
// ============================================================
#[derive(Deserialize, Debug)]
struct Config {
    #[serde(default = "default_profile_dir")]
    profile_dir: String,

    #[serde(default = "default_chromium_path")]
    chromium_path: String,

    #[serde(default = "default_result_dir")]
    result_dir: String,

    #[serde(default = "default_max_pages")]
    max_pages: u32,

    #[serde(default = "default_max_consecutive_no_next")]
    max_consecutive_no_next: u32,

    #[serde(default = "default_search_queries")]
    search_queries: Vec<String>,
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

fn load_config() -> Config {
    let config_path = get_base_path("Config.toml");
    if config_path.exists() {
        println!("設定ファイル読み込み: {:?}", config_path);
        if let Ok(content) = fs::read_to_string(&config_path) {
            if let Ok(cfg) = toml::from_str(&content) {
                println!("設定ファイル読み込み成功");
                return cfg;
            }
        }
    }
    println!("設定ファイル読み込み失敗。デフォルト使用。");
    Config::default()
}

#[derive(Serialize, Deserialize, Debug)]
struct SearchResult {
    rank: usize,
    title: String,
    url: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct PageResult {
    query: String,
    page: u32,
    timestamp: String,
    result_count: usize,
    results: Vec<SearchResult>,
}

fn format_duration(start: DateTime<Local>, end: DateTime<Local>) -> String {
    let duration = end.signed_duration_since(start);
    let total_seconds = duration.num_seconds();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let millis = duration.num_milliseconds() % 1000;
    if hours > 0 {
        format!("{}時間{}分{}秒", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}分{}秒", minutes, seconds)
    } else {
        format!("{}.{:03}秒", seconds, millis)
    }
}

fn get_base_path(relative: &str) -> PathBuf {
    if cfg!(debug_assertions) {
        let current_dir = env::current_dir().expect("カレントディレクトリ取得失敗");
        current_dir.join(relative)
    } else {
        let exe_path = env::current_exe().expect("実行ファイルパス取得失敗");
        exe_path.parent().unwrap().join(relative)
    }
}

fn init_profile_dir(config: &Config) -> Result<PathBuf> {
    let path = get_base_path(&config.profile_dir);
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn clear_profile_dir(config: &Config) -> Result<()> {
    let path = get_base_path(&config.profile_dir);
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    Ok(())
}

fn init_result_dir(config: &Config, start_time: DateTime<Local>) -> Result<PathBuf> {
    let time_str = start_time.format("%Y-%m-%d-%H-%M-%S").to_string();
    let path = get_base_path(&config.result_dir).join(&time_str);
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn init_query_result_dir(result_base: &PathBuf, query: &str) -> Result<PathBuf> {
    let safe_query = query
        .replace('/', "_")
        .replace('\\', "_")
        .replace(':', "_")
        .replace('*', "_")
        .replace('?', "_")
        .replace('"', "_")
        .replace('<', "_")
        .replace('>', "_")
        .replace('|', "_");
    let path = result_base.join(&safe_query);
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn save_search_results_json(
    query_dir: &PathBuf,
    query: &str,
    page_num: u32,
    results: &[(String, String)],
) -> Result<()> {
    let file_path = query_dir.join(format!("{}.json", page_num));
    let search_results: Vec<SearchResult> = results
        .iter()
        .enumerate()
        .map(|(i, (title, url))| SearchResult {
            rank: i + 1,
            title: title.clone(),
            url: url.clone(),
        })
        .collect();
    let page_result = PageResult {
        query: query.to_string(),
        page: page_num,
        timestamp: Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        result_count: search_results.len(),
        results: search_results,
    };
    let json = serde_json::to_string_pretty(&page_result)?;
    let mut file = fs::File::create(&file_path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

fn extract_search_results(html: &str) -> Vec<(String, String)> {
    let document = Html::parse_document(html);
    let selector = Selector::parse(r#"a[jsname="UWckNb"]"#).unwrap();
    let mut results = Vec::new();
    let mut seen_urls: HashSet<String> = HashSet::new();
    
    for element in document.select(&selector) {
        let url = element.value().attr("href").unwrap_or("").to_string();
        let title_selector = Selector::parse("h3").unwrap();
        let title = element
            .select(&title_selector)
            .next()
            .map(|h3| h3.text().collect::<String>())
            .unwrap_or_default();
        
        // URL重複チェック（上位優先で残す）
        if !url.is_empty() && !title.is_empty() && !seen_urls.contains(&url) {
            seen_urls.insert(url.clone());
            results.push((title, url));
        }
    }
    results
}

struct BrowserManager<'a> {
    browser: Option<Browser>,
    config: &'a Config,
}

impl<'a> BrowserManager<'a> {
    fn new(config: &'a Config) -> Self {
        Self {
            browser: None,
            config,
        }
    }

    fn get_or_create(&mut self) -> Result<&Browser> {
        if self.browser.is_none() {
            self.browser = Some(launch_browser(self.config)?);
        }
        Ok(self.browser.as_ref().unwrap())
    }

    fn restart(&mut self) -> Result<&Browser> {
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
fn launch_browser(config: &Config) -> Result<Browser> {
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
// CDP Stealth設定
// ============================================================
fn setup_stealth_cdp(tab: &Tab) -> Result<()> {
    use headless_chrome::protocol::cdp::Emulation::{UserAgentBrandVersion, UserAgentMetadata};
    use headless_chrome::protocol::cdp::Network::SetUserAgentOverride;

    tab.call_method(SetUserAgentOverride {
        user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36".to_string(),
        accept_language: Some("ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7".to_string()),
        platform: Some("Win32".to_string()),
        user_agent_metadata: Some(UserAgentMetadata {
            platform: "Windows".to_string(),
            platform_version: "19.0.0.0".to_string(),
            architecture: "x86".to_string(),
            model: "".to_string(),
            mobile: false,
            bitness: Some("64".to_string()),
            wow_64: Some(false),
            full_version_list: Some(vec![
                UserAgentBrandVersion { brand: "Chromium".to_string(), version: "143.0.7499.41".to_string() },
                UserAgentBrandVersion { brand: "Google Chrome".to_string(), version: "143.0.7499.41".to_string() },
                UserAgentBrandVersion { brand: "Not/A)Brand".to_string(), version: "99.0.0.0".to_string() },
            ]),
            full_version: Some("143.0.7499.41".to_string()),
            brands: Some(vec![
                UserAgentBrandVersion { brand: "Chromium".to_string(), version: "143".to_string() },
                UserAgentBrandVersion { brand: "Google Chrome".to_string(), version: "143".to_string() },
                UserAgentBrandVersion { brand: "Not/A)Brand".to_string(), version: "99".to_string() },
            ]),
            form_factors: None,
        }),
    })?;

    Ok(())
}

// ============================================================
// JavaScript Stealth Injection（Ver 1.2 強化版）
// ============================================================
fn inject_stealth_scripts(tab: &Tab) -> Result<()> {
    use headless_chrome::protocol::cdp::Page::AddScriptToEvaluateOnNewDocument;

    let scripts = vec![
        // ===== 基本Stealth =====
        
        // webdriver検出回避
        r#"Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });"#,

        // chrome オブジェクト偽装
        r#"window.chrome = {
            runtime: {
                connect: function() {},
                sendMessage: function() {},
                onMessage: { addListener: function() {} },
                onConnect: { addListener: function() {} },
                PlatformOs: { MAC: 'mac', WIN: 'win', ANDROID: 'android', CROS: 'cros', LINUX: 'linux', OPENBSD: 'openbsd' },
                PlatformArch: { ARM: 'arm', X86_32: 'x86-32', X86_64: 'x86-64', MIPS: 'mips', MIPS64: 'mips64' },
                PlatformNaclArch: { ARM: 'arm', X86_32: 'x86-32', X86_64: 'x86-64', MIPS: 'mips', MIPS64: 'mips64' },
                RequestUpdateCheckStatus: { THROTTLED: 'throttled', NO_UPDATE: 'no_update', UPDATE_AVAILABLE: 'update_available' },
                OnInstalledReason: { INSTALL: 'install', UPDATE: 'update', CHROME_UPDATE: 'chrome_update', SHARED_MODULE_UPDATE: 'shared_module_update' },
                OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' }
            },
            csi: function() { return {}; },
            loadTimes: function() { return {}; }
        };"#,

        // permissions.query 偽装（通知用）
        r#"const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );"#,

        // plugins/mimeTypes 偽装
        r#"Object.defineProperty(navigator, 'plugins', {
            get: () => {
                const plugins = [
                    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                    { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
                ];
                plugins.length = 3;
                return plugins;
            }
        });
        Object.defineProperty(navigator, 'mimeTypes', {
            get: () => {
                const mimeTypes = [
                    { type: 'application/pdf', suffixes: 'pdf', description: 'Portable Document Format' },
                    { type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' },
                    { type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' },
                    { type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }
                ];
                mimeTypes.length = 4;
                return mimeTypes;
            }
        });"#,

        // languages 偽装
        r#"Object.defineProperty(navigator, 'languages', { get: () => ['ja-JP', 'ja', 'en-US', 'en'] });"#,

        // hardwareConcurrency 偽装
        r#"Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 12 });"#,

        // deviceMemory 偽装
        r#"Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });"#,

        // WebGL 偽装
        r#"const getParameterOriginal = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Google Inc. (NVIDIA)';
            if (parameter === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce RTX 2080 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)';
            return getParameterOriginal.call(this, parameter);
        };
        const getParameterOriginal2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Google Inc. (NVIDIA)';
            if (parameter === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce RTX 2080 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)';
            return getParameterOriginal2.call(this, parameter);
        };"#,

        // Brave/Firefox 検出回避
        r#"Object.defineProperty(navigator, 'brave', { get: () => undefined });
        delete window.InstallTrigger;"#,

        // Function.prototype.toString 偽装
        r#"const nativeToString = Function.prototype.toString;
        const customFunctions = new WeakSet();
        const proxyHandler = {
            apply: function(target, thisArg, args) {
                if (customFunctions.has(thisArg)) return 'function () { [native code] }';
                return nativeToString.apply(thisArg, args);
            }
        };
        Function.prototype.toString = new Proxy(nativeToString, proxyHandler);
        customFunctions.add(Function.prototype.toString);"#,

        // ===== 位置情報ポップアップ完全ブロック（Ver 1.2 新機能） =====

        // A. CSS強制非表示
        r#"
        (function() {
            const style = document.createElement('style');
            style.textContent = `
                /* 位置情報ダイアログ本体 */
                div.gTMtLb[id="lb"],
                div[role="dialog"][aria-labelledby="lcMwfd"],
                div.qk7LXc.JHqNkc,
                /* update-location コンポーネント全体 */
                update-location,
                /* 位置情報スナックバー */
                location-snackbar-with-learn-more,
                /* ライトボックス背景 */
                div.kJFf0c.KUf18 {
                    display: none !important;
                    visibility: hidden !important;
                    opacity: 0 !important;
                    pointer-events: none !important;
                }
            `;
            (document.head || document.documentElement).appendChild(style);
        })();
        "#,

        // B. Geolocation API完全無効化
        r#"
        (function() {
            // Geolocation API無効化
            if (navigator.geolocation) {
                navigator.geolocation.getCurrentPosition = function(success, error) {
                    if (error) error({ code: 1, message: 'User denied Geolocation' });
                };
                navigator.geolocation.watchPosition = function(success, error) {
                    if (error) error({ code: 1, message: 'User denied Geolocation' });
                    return 0;
                };
                navigator.geolocation.clearWatch = function() {};
            }
            
            // permissions.query偽装（geolocationを常にdenied）
            const origPermQuery = navigator.permissions.query.bind(navigator.permissions);
            navigator.permissions.query = function(descriptor) {
                if (descriptor.name === 'geolocation') {
                    return Promise.resolve({ 
                        state: 'denied',
                        onchange: null,
                        addEventListener: function() {},
                        removeEventListener: function() {}
                    });
                }
                return origPermQuery(descriptor);
            };
        })();
        "#,

        // C. 精密クリック + 常時監視（強化版）
        r#"
        (function() {
            const SELECTORS = {
                dialog: 'div[role="dialog"][aria-labelledby="lcMwfd"]',
                dialogAlt: 'div.qk7LXc.JHqNkc[role="dialog"]',
                lightbox: 'div.gTMtLb#lb',
                laterButton: [
                    'g-raised-button[jsaction="click:O6N1Pb"]',
                    'div.mpQYc g-raised-button'
                ],
                closeButton: 'a[aria-label="閉じる"]'
            };
            
            let lastDismissTime = 0;
            const DEBOUNCE_MS = 100;
            
            const dismiss = () => {
                const now = Date.now();
                if (now - lastDismissTime < DEBOUNCE_MS) return false;
                lastDismissTime = now;
                
                const dialog = document.querySelector(SELECTORS.dialog) ||
                               document.querySelector(SELECTORS.dialogAlt);
                if (!dialog) return false;
                
                const style = window.getComputedStyle(dialog);
                if (style.display === 'none' || 
                    style.visibility === 'hidden' || 
                    parseFloat(style.opacity) === 0) {
                    return false;
                }
                
                // 「後で」ボタン検索・クリック
                for (const sel of SELECTORS.laterButton) {
                    const btn = dialog.querySelector(sel) || document.querySelector(sel);
                    if (btn) {
                        btn.click();
                        btn.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                        console.log('[Gnezdo] 「後で」クリック成功');
                        setTimeout(remove, 50);
                        return true;
                    }
                }
                
                // 閉じるボタン（フォールバック）
                const close = document.querySelector(SELECTORS.closeButton);
                if (close) { 
                    close.click(); 
                    console.log('[Gnezdo] 「閉じる」クリック');
                    setTimeout(remove, 50);
                    return true; 
                }
                
                // テキストベース検索（最終手段）
                const all = dialog.querySelectorAll('div[role="button"], button, g-raised-button');
                for (const b of all) {
                    const text = b.innerText?.trim();
                    if (text === '後で' || text === 'Later' || text === 'Not now') {
                        b.click();
                        console.log('[Gnezdo] テキストマッチでクリック:', text);
                        setTimeout(remove, 50);
                        return true;
                    }
                }
                return false;
            };
            
            const remove = () => {
                const lb = document.querySelector(SELECTORS.lightbox);
                if (lb) { 
                    lb.style.display = 'none'; 
                    lb.remove(); 
                    console.log('[Gnezdo] ライトボックス削除');
                }
            };
            
            // ===== 監視機構（多重化） =====
            
            // 1. 初回実行（ページロード後）
            setTimeout(dismiss, 300);
            setTimeout(dismiss, 600);
            setTimeout(dismiss, 1000);
            
            // 2. MutationObserver（DOM変更検知）
            const observer = new MutationObserver((mutations) => {
                for (const m of mutations) {
                    if (m.type === 'childList' && m.addedNodes.length > 0) {
                        dismiss();
                        return;
                    }
                    if (m.type === 'attributes') {
                        const t = m.target;
                        if (t.id === 'lb' || 
                            t.matches?.('[role="dialog"]') ||
                            t.classList?.contains('gTMtLb')) {
                            dismiss();
                            return;
                        }
                    }
                }
            });
            
            const startObserver = () => {
                observer.observe(document.body, {
                    childList: true,
                    subtree: true,
                    attributes: true,
                    attributeFilter: ['style', 'class', 'aria-hidden', 'hidden']
                });
            };
            
            // 3. 定期チェック（500ms間隔）
            setInterval(dismiss, 500);
            
            // 4. イベントベース監視
            ['scroll', 'click', 'keydown', 'mousemove'].forEach(event => {
                document.addEventListener(event, () => {
                    setTimeout(dismiss, 50);
                }, { passive: true, capture: true });
            });
            
            // 5. フォーカス/表示状態変更時
            window.addEventListener('focus', dismiss);
            document.addEventListener('visibilitychange', () => {
                if (document.visibilityState === 'visible') dismiss();
            });
            
            // 6. ページ遷移系イベント
            window.addEventListener('popstate', dismiss);
            window.addEventListener('hashchange', dismiss);
            
            // 7. requestAnimationFrame監視（最初の30秒間のみ）
            let rafActive = true;
            const rafCheck = () => {
                if (!rafActive) return;
                dismiss();
                setTimeout(() => requestAnimationFrame(rafCheck), 200);
            };
            requestAnimationFrame(rafCheck);
            setTimeout(() => { rafActive = false; }, 30000);
            
            // 開始
            if (document.body) {
                startObserver();
            } else {
                document.addEventListener('DOMContentLoaded', () => {
                    startObserver();
                    dismiss();
                });
            }
            
            console.log('[Gnezdo] 位置情報ポップアップ監視開始 (Ver 1.3)');
        })();
        "#,
    ];

    for script in scripts {
        tab.call_method(AddScriptToEvaluateOnNewDocument {
            source: script.to_string(),
            world_name: None,
            include_command_line_api: None,
            run_immediately: None,
        })?;
    }

    Ok(())
}

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

    let mut manager = BrowserManager::new(&config);
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

// ============================================================
// 全クエリ実行
// ============================================================
fn run_all_queries(
    manager: &mut BrowserManager,
    program_start: DateTime<Local>,
    result_base: &PathBuf,
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
// アクティブタブ取得
// ============================================================
fn get_active_tab(manager: &mut BrowserManager) -> Result<Arc<Tab>> {
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

// ============================================================
// 単一クエリ実行
// ============================================================
fn execute_single_query(
    tab: &Arc<Tab>,
    query: &str,
    query_dir: &PathBuf,
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
        let results = extract_search_results(&html);

        if !results.is_empty() {
            save_search_results_json(query_dir, query, page_num, &results)?;
        } else {
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

// ============================================================
// 人間らしいスクロール
// ============================================================
fn human_scroll_to_bottom_medium(tab: &Arc<Tab>) -> Result<()> {
    let mut rng = WyRand::new();

    let mut mode_steps_remaining = 0;
    let mut current_mode: u8 = 0;

    loop {
        let scroll_info = tab.evaluate(
            "JSON.stringify({ scrollY: window.scrollY, innerHeight: window.innerHeight, scrollHeight: document.body.scrollHeight })",
            false
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
                rng.generate_range(175_i32..=200),
                rng.generate_range(20_u64..=25),
            ),
            1 => (
                rng.generate_range(200_i32..=225),
                rng.generate_range(15_u64..=20),
            ),
            _ => (
                rng.generate_range(225_i32..=250),
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
fn human_pause_with_keepalive(tab: &Arc<Tab>, total_ms: u64) -> Result<()> {
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
fn human_type_medium(tab: &Arc<Tab>, text: &str) -> Result<()> {
    let mut rng = WyRand::new();

    for c in text.chars() {
        tab.send_character(&c.to_string())?;
        let delay = rng.generate_range(75_u64..=300);
        thread::sleep(Duration::from_millis(delay));
    }

    Ok(())
}