#!/bin/bash

# 環境変数の設定
export TWITTER_BEARER_TOKEN='your_bearer_token'
export S3_BUCKET_NAME='momoiro-twitter-test'

# Lambda関数の実行
aws lambda invoke \
    --function-name momoiro-twitter-fetcher \
    --payload '{}' \
    response.json

# レスポンスの表示
cat response.json
rm response.json

# 保存されたファイルの確認
echo "保存されたファイルの一覧:"
aws s3 ls s3://${S3_BUCKET_NAME}/tweets/ --recursive 