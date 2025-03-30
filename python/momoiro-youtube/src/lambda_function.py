import json
import os
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Any
import boto3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# S3クライアントの初期化
s3 = boto3.client('s3')
BUCKET_NAME = os.environ['S3_BUCKET_NAME']

# YouTube APIクライアントの初期化
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# ももクロの公式チャンネルID
CHANNEL_IDS = [
    'UC7pcEjI2cdH1SKR0VPxVxZw',  # ももいろクローバーZ
    'UCQhK0M0B5QX0jc2xmfC_syw',  # ももクロちゃんZ
    'UCf5t4M5HJBvnwpqoe8R8GIg'   # STARDUST CHANNEL
]

def format_rfc3339(dt: datetime) -> str:
    """datetime オブジェクトをRFC3339形式の文字列に変換"""
    # UTCに変換してからフォーマット
    utc_dt = dt.astimezone(pytz.UTC)
    return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def get_channel_videos(channel_id: str, published_after: datetime) -> List[Dict[str, Any]]:
    """指定したチャンネルの動画情報を取得"""
    try:
        # UTC時刻に変換
        published_after_utc = published_after.astimezone(pytz.UTC)
        
        # 検索リクエストを実行
        search_response = youtube.search().list(
            channelId=channel_id,
            part='id,snippet',
            order='date',
            publishedAfter=format_rfc3339(published_after_utc),
            type='video',
            maxResults=50
        ).execute()

        videos = []
        for item in search_response.get('items', []):
            if item['id']['kind'] == 'youtube#video':
                # 動画の詳細情報を取得
                video_response = youtube.videos().list(
                    part='statistics,contentDetails',
                    id=item['id']['videoId']
                ).execute()

                if video_response['items']:
                    video_stats = video_response['items'][0]
                    video_data = {
                        'video_id': item['id']['videoId'],
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'],
                        'published_at': item['snippet']['publishedAt'],
                        'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
                        'channel_id': channel_id,
                        'channel_title': item['snippet']['channelTitle'],
                        'statistics': video_stats['statistics'],
                        'duration': video_stats['contentDetails']['duration'],
                        'fetched_at': datetime.now(pytz.UTC).isoformat()
                    }
                    videos.append(video_data)

        return videos

    except HttpError as e:
        print(f'An HTTP error {e.resp.status} occurred: {e.content}')
        return []

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        # 東京タイムゾーン
        tz = pytz.timezone('Asia/Tokyo')
        current_time = datetime.now(tz)
        # 過去24時間の動画を取得するため、現在時刻から24時間前を計算
        one_day_ago = current_time - timedelta(days=7)  # 1週間前までの動画を取得するように変更

        # 全チャンネルの動画を取得
        all_videos = []
        for channel_id in CHANNEL_IDS:
            videos = get_channel_videos(channel_id, one_day_ago)
            all_videos.extend(videos)

        if all_videos:
            # S3に保存するファイル名の生成
            date_str = current_time.strftime('%Y/%m/%d/%H')
            file_name = f"youtube_videos/{date_str}/videos_{current_time.strftime('%M')}.json"
            
            # JSONデータをS3にアップロード
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(all_videos, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully fetched and saved videos',
                'video_count': len(all_videos),
                'time_range': {
                    'start': one_day_ago.isoformat(),
                    'end': current_time.isoformat()
                }
            }, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        } 