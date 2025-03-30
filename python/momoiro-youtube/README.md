# Momoiro YouTube Data Fetcher

ももいろクローバーZの公式YouTubeチャンネルから動画情報を取得し、S3に保存するプログラム。

## ディレクトリ構成

```
.
├── src/                  # ソースコードディレクトリ
│   ├── __init__.py
│   └── lambda_function.py
├── scripts/             # 実行スクリプトディレクトリ
│   ├── run_local.py     # ローカル実行用スクリプト
│   ├── run_search.sh    # 検索実行用スクリプト
│   └── setup.sh         # セットアップスクリプト
├── config/             # 設定ファイルディレクトリ
│   ├── .env           # 環境変数設定
│   └── dynamodb_table.json  # DynamoDBテーブル定義
└── requirements.txt    # 依存関係
```

## セットアップ

1. 必要なパッケージをインストール：
```bash
pip install -r requirements.txt
```

2. 環境変数の設定：
`config/.env`ファイルを作成し、以下の内容を設定：
```
YOUTUBE_API_KEY=your_api_key
S3_BUCKET_NAME=your_bucket_name
```

## 実行方法

ローカルでの実行：
```bash
python scripts/run_local.py
```

検索の実行：
```bash
./scripts/run_search.sh
``` 