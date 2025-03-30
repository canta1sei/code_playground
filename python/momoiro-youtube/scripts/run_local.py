import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートを追加
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

# .envファイルのパスを設定
env_path = os.path.join(project_root, 'config', '.env')
print(f"Loading .env from: {env_path}")  # デバッグ用
load_dotenv(env_path, override=True)

# 環境変数の値を確認
print(f"YOUTUBE_API_KEY: {os.getenv('YOUTUBE_API_KEY')}")  # デバッグ用
print(f"S3_BUCKET_NAME: {os.getenv('S3_BUCKET_NAME')}")    # デバッグ用

# Lambda関数をインポート
from src.lambda_function import lambda_handler

# Lambda関数の実行
result = lambda_handler({}, None)
print(result) 