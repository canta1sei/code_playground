# S3 API

AWS S3との連携を行うSpring Bootアプリケーション

## 前提条件

- Java 17以上
- Maven 3.6以上
- AWSアカウントとS3バケット

## セットアップ

1. リポジトリをクローン
```bash
git clone [repository-url]
cd s3-api
```

2. 環境変数の設定
以下の環境変数を設定してください：
- `AWS_ACCESS_KEY_ID`: AWSアクセスキーID
- `AWS_SECRET_KEY`: AWSシークレットキー

3. アプリケーションの設定
`application.yml.example`を`application.yml`にコピーし、必要に応じて設定を変更してください。

## ビルドと実行

```bash
mvn clean install
mvn spring:boot run
```

## APIエンドポイント

- `GET /api/files`: バケット内のファイル一覧を取得
- `POST /api/files`: ファイルをアップロード
- `POST /api/buckets`: 新しいバケットを作成

## テスト

```bash
mvn test
```

## セキュリティ

- AWS認証情報は環境変数として設定してください
- 本番環境では適切なセキュリティ設定を行ってください 