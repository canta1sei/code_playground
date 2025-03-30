import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートを追加
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

# .envファイルのパスを設定
env_path = os.path.join(project_root, 'config', '.env')
load_dotenv(env_path)

# Lambda関数をインポート
from src.lambda_function import lambda_handler

# Lambda関数の実行
result = lambda_handler({}, None)
print(result) 