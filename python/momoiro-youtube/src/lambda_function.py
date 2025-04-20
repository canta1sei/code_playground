import json
import os
from datetime import datetime
import pytz
from typing import Dict, Any
import boto3
from googleapiclient.discovery import build
from dotenv import load_dotenv
import csv

# 環境変数の読み込み
load_dotenv()

# S3クライアントの初期化
s3 = boto3.client('s3', region_name='ap-northeast-1')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# YouTube APIクライアントの初期化
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# ももクロの公式チャンネルID
CHANNEL_ID = 'UCMzR-mdZmIi-ZA7VQdDUyUQ'  # ももいろクローバーZ Official Channel

def get_channel_videos_for_year(channel_id: str, year: int) -> list:
    """指定された年のチャンネルの動画を取得"""
    videos = []
    next_page_token = None

    # 年を開始日と終了日に変換
    start_date = datetime(year, 1, 1).isoformat() + 'Z'
    end_date = datetime(year, 12, 31, 23, 59, 59).isoformat() + 'Z'

    while True:
        try:
            # チャンネルの動画を検索
            response = youtube.search().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50,
                order='date',
                publishedAfter=start_date,
                publishedBefore=end_date,
                type='video',
                pageToken=next_page_token
            ).execute()

            # 動画IDを取得
            video_ids = [item['id']['videoId'] for item in response['items']]

            # 動画の詳細情報を取得
            if video_ids:
                video_response = youtube.videos().list(
                    part='snippet,statistics',
                    id=','.join(video_ids)
                ).execute()

                videos.extend(video_response['items'])

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        except Exception as e:
            print(f"エラーが発生しました: {e}")
            break

    return videos

def save_videos_to_csv(videos: list, year: int, fetched_at: str) -> str:
    """動画情報をCSV形式で保存"""
    csv_buffer = []
    
    # CSVヘッダー
    headers = [
        'videoId',
        'title',
        'publishedAt',
        'channelId',
        'channelTitle',
        'viewCount',
        'likeCount',
        'commentCount',
        'year',
        'fetched_at'
    ]
    csv_buffer.append(','.join(headers) + '\n')

    # 動画情報を追加
    for video in videos:
        snippet = video['snippet']
        statistics = video['statistics']
        row = [
            video['id'],
            snippet['title'].replace(',', '，').replace('\n', ' '),
            snippet['publishedAt'],
            snippet['channelId'],
            snippet['channelTitle'].replace(',', '，'),
            statistics.get('viewCount', '0'),
            statistics.get('likeCount', '0'),
            statistics.get('commentCount', '0'),
            str(year),
            fetched_at
        ]
        csv_buffer.append(','.join(row) + '\n')

    return ''.join(csv_buffer)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        # 東京タイムゾーン
        tz = pytz.timezone('Asia/Tokyo')
        # 実行時の現在時刻
        current_time = datetime.now(tz)
        
        # 取得対象の年を設定（2008年から現在まで）
        start_year = 2008
        end_year = current_time.year

        all_videos = []
        for year in range(start_year, end_year + 1):
            print(f"\nFetching videos for year {year}")
            videos = get_channel_videos_for_year(CHANNEL_ID, year)
            
            if videos:
                # 取得時刻をUTCで取得
                fetched_at = datetime.now(pytz.UTC)
                
                # 基本的な統計情報を計算
                total_views = sum(int(v['statistics'].get('viewCount', 0)) for v in videos)
                total_likes = sum(int(v['statistics'].get('likeCount', 0)) for v in videos)
                total_comments = sum(int(v['statistics'].get('commentCount', 0)) for v in videos)
                
                # メタデータを追加
                metadata = {
                    'year': year,
                    'total_videos': len(videos),
                    'total_views': total_views,
                    'total_likes': total_likes,
                    'total_comments': total_comments,
                    'average_views': total_views / len(videos) if videos else 0,
                    'average_likes': total_likes / len(videos) if videos else 0,
                    'average_comments': total_comments / len(videos) if videos else 0,
                    'fetched_at': fetched_at.isoformat()
                }
                
                # JSONファイル名の生成
                json_file_name = f"youtube_videos/videos_{year}_{len(videos)}.json"
                
                # JSONデータをS3にアップロード
                json_data = {
                    'metadata': metadata,
                    'videos': videos
                }
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=json_file_name,
                    Body=json.dumps(json_data, ensure_ascii=False, indent=2),
                    ContentType='application/json'
                )

                # CSVファイル名の生成
                csv_file_name = f"youtube_videos/videos_{year}_{len(videos)}.csv"

                # CSVデータを生成してS3にアップロード
                csv_data = save_videos_to_csv(videos, year, fetched_at.isoformat())
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=csv_file_name,
                    Body=csv_data,
                    ContentType='text/csv'
                )
                
                all_videos.extend(videos)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully fetched and saved videos',
                'video_count': len(all_videos),
                'channel': 'ももいろクローバーZ Official Channel',
                'years_processed': list(range(start_year, end_year + 1))
            }, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        } 