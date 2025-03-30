import os
from dotenv import load_dotenv
from lambda_function import lambda_handler

# .envファイルから環境変数を読み込む
load_dotenv()

# Lambda関数の実行
result = lambda_handler({}, None)
print(result) 