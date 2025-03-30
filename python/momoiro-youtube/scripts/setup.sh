#!/bin/bash

# S3バケットの作成
aws s3api create-bucket \
    --bucket momoiro-twitter-data \
    --region ap-northeast-1 \
    --create-bucket-configuration LocationConstraint=ap-northeast-1

# Lambda関数の環境変数設定
aws lambda update-function-configuration \
    --function-name momoiro-twitter-fetcher \
    --environment "Variables={
        TWITTER_BEARER_TOKEN='your_bearer_token',
        S3_BUCKET_NAME='momoiro-twitter-data'
    }"

# EventBridgeのルール作成（5分ごとに実行）
aws events put-rule \
    --name momoiro-twitter-fetcher-rule \
    --schedule-expression "rate(5 minutes)" \
    --state ENABLED

# EventBridgeのターゲット設定
aws events put-targets \
    --rule momoiro-twitter-fetcher-rule \
    --targets "Id"="1","Arn"="arn:aws:lambda:ap-northeast-1:YOUR_ACCOUNT_ID:function:momoiro-twitter-fetcher" 