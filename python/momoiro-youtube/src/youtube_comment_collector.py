import os
import json
import sys
import argparse
from datetime import datetime
import boto3
from googleapiclient.discovery import build
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# YouTube APIの設定
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# S3の設定
s3 = boto3.client('s3', region_name='ap-northeast-1')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME_GET_COMMENT')

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

def save_to_s3(comments, video_id):
    """コメントをS3に保存"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"comments_{video_id}_{timestamp}.json"
    
    # コメントをJSON形式に変換
    data = {
        'video_id': video_id,
        'timestamp': timestamp,
        'comments': comments
    }
    
    try:
        # S3にアップロード
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"youtube_comments/{filename}",
            Body=json.dumps(data, ensure_ascii=False, indent=2)
        )
        print(f"コメントを {filename} としてS3に保存しました")
        
    except Exception as e:
        print(f"S3への保存中にエラーが発生しました: {e}")

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