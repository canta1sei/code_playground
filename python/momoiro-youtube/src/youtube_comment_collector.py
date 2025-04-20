import os
import json
import sys
import argparse
from datetime import datetime
import boto3
from googleapiclient.discovery import build
from dotenv import load_dotenv
import csv

# 環境変数の読み込み
load_dotenv()

# YouTube APIの設定
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# S3の設定
s3 = boto3.client('s3', region_name='ap-northeast-1')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME_GET_COMMENT')

def get_date_folder():
    """実行日付のフォルダ名を生成（yyyy=YYYY/mm=MM/dd=DD形式）"""
    current_time = datetime.now()
    return f"yyyy={current_time.year}/mm={current_time.month:02d}/dd={current_time.day:02d}"

def get_comments(video_id):
    """動画のコメントを取得"""
    comments = []
    next_page_token = None
    
    while True:
        try:
            # コメントスレッドを取得
            response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token
            ).execute()
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': comment['authorDisplayName'],
                    'text': comment['textDisplay'],
                    'likeCount': comment['likeCount'],
                    'publishedAt': comment['publishedAt']
                })
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
        except Exception as e:
            print(f"エラーが発生しました: {e}")
            break
    
    return comments

def save_json_to_s3(comments, video_id, timestamp):
    """コメントをJSON形式でS3に保存"""
    json_filename = f"comments_{video_id}_{timestamp}.json"
    date_folder = get_date_folder()
    
    # JSON形式で保存
    data = {
        'video_id': video_id,
        'timestamp': timestamp,
        'comments': comments
    }

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"json/{date_folder}/{json_filename}",
            Body=json.dumps(data, ensure_ascii=False, indent=2)
        )
        print(f"コメントを json/{date_folder}/{json_filename} としてS3に保存しました")
    except Exception as e:
        print(f"JSONのS3への保存中にエラーが発生しました: {e}")

def save_csv_to_s3(comments, video_id, timestamp):
    """コメントをCSV形式でS3に保存"""
    csv_filename = f"comments_{video_id}_{timestamp}.csv"
    date_folder = get_date_folder()
    
    try:
        # CSVデータの作成
        csv_buffer = []
        csv_buffer.append("author,publishedAt,likeCount,text\n")
        for comment in comments:
            # テキスト内のカンマと改行をエスケープ
            text = comment['text'].replace(',', '，').replace('\n', ' ')
            csv_buffer.append(f"{comment['author']},{comment['publishedAt']},{comment['likeCount']},{text}\n")

        # S3にアップロード
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"csv/{date_folder}/{csv_filename}",
            Body=''.join(csv_buffer)
        )
        print(f"コメントを csv/{date_folder}/{csv_filename} としてS3に保存しました")
    except Exception as e:
        print(f"CSVのS3への保存中にエラーが発生しました: {e}")

def save_to_s3(comments, video_id):
    """コメントをS3に保存（JSONとCSV形式）"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # JSON形式で保存
    save_json_to_s3(comments, video_id, timestamp)
    
    # CSV形式で保存
    save_csv_to_s3(comments, video_id, timestamp)

def main(video_id):
    # コマンドライン引数の設定
    print(f"動画ID: {video_id} のコメントを取得中...")

    comments = get_comments(video_id)
    print(f"{len(comments)}件のコメントを取得しました")

    save_to_s3(comments, video_id)

if __name__ == "__main__":
    # コマンドライン引数の設定
    parser = argparse.ArgumentParser(description='YouTube動画のコメントを取得してS3に保存します')
    parser.add_argument('video_id', help='YouTube動画ID')
    args = parser.parse_args()

    main(args.video_id) 