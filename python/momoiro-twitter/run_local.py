import os
from lambda_function import lambda_handler

# 環境変数の設定
os.environ['YOUTUBE_API_KEY'] = 'AIzaSyAgkJ53Tt94u9Pqk9C1S-lbGv26OVmgth8'
os.environ['S3_BUCKET_NAME'] = 'momoiro-twitter-test'

# Lambda関数の実行
result = lambda_handler({}, None)
print(result) 