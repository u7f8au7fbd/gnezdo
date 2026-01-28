Gnezdo
======

概要
----
Google 検索結果を Chromium で巡回し、抽出した結果を JSON で保存する CLI ツールです。

必須: `chromium/` フォルダ
------------------------
このプロジェクトは同梱の Chromium を使います。`config.toml` の `chromium_path` が指す
`chromium/chrome.exe` と、その実行に必要な同梱ファイル一式が必要です。

最低限の前提:
- `chromium/chrome.exe` が存在する
- `chromium/profile/` が作成可能（初回起動時に自動作成）

典型的な配置例（参考）
----------------------
Chromium の配布物一式が `chromium/` に置かれている想定です。
最低限 `chrome.exe` と、その隣にあるバージョンディレクトリ配下の DLL/PAK などが必要です。

```
chromium/
├─ chrome.exe
├─ chrome_proxy.exe
├─ profile/               # 初回起動で自動作成される
└─ <version>/             # 例: 143.0.7499.110
```

ディレクトリ構造
----------------
```
.
├─ Cargo.toml
├─ Cargo.lock
├─ config.toml
├─ chromium/             # Chromium 実行環境（必須）
├─ result/               # 実行結果の保存先（自動作成）
└─ src/
   ├─ main.rs
   ├─ scrape.rs          # 取得/保存の編集点
   └─ core/              # それ以外の内部実装
```

`config.toml` の使い方
----------------------
`config.toml` を編集すると挙動を変えられます。主な項目:

```
# Chromium プロファイル保存先（chromium 内推奨）
profile_dir = "chromium/profile"

# Chromium 実行ファイルパス
chromium_path = "chromium/chrome.exe"

# 検索結果保存先ディレクトリ
result_dir = "result"

# 巡回する最大ページ数
max_pages = 10

# 「次へ」が連続で見つからない場合の閾値（この回数で一時停止）
max_consecutive_no_next = 2

# 検索クエリ一覧
search_queries = [
  "水素",
  "ヘリウム",
  "リチウム",
  "ベリリウム",
  ...
]
```

ポイント:
- `chromium_path` は `chromium/chrome.exe` を指す必要があります
- `profile_dir` と `result_dir` は自動作成されます
- `search_queries` を増やすと巡回対象が増えます
