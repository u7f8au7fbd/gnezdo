use anyhow::Result;
use headless_chrome::protocol::cdp::Page::AddScriptToEvaluateOnNewDocument;
use headless_chrome::protocol::cdp::{Emulation::UserAgentBrandVersion, Emulation::UserAgentMetadata};
use headless_chrome::protocol::cdp::Network::SetUserAgentOverride;
use headless_chrome::Tab;

// ============================================================
// CDP Stealth設定
// ============================================================
pub fn setup_stealth_cdp(tab: &Tab) -> Result<()> {
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
pub fn inject_stealth_scripts(tab: &Tab) -> Result<()> {
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
                        onchange: null
                    });
                }
                return origPermQuery(descriptor);
            };
        })();
        "#,

        // C. 位置情報ポップアップ自動閉じ
        r#"
        (function() {
            const SELECTORS = {
                dialog: 'div.gTMtLb[id="lb"]',
                dialogAlt: 'div[role="dialog"][aria-labelledby="lcMwfd"]',
                lightbox: 'div.kJFf0c.KUf18',
                laterButton: [
                    'button[aria-label="後で"]',
                    'button[aria-label="Later"]',
                    'button[jsname="VIftHc"]',
                    'g-raised-button[jsname="Qx7uuf"]',
                    'button[jsname="Cuz2Ue"]'
                ],
                closeButton: 'button[aria-label="閉じる"]'
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
