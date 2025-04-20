import json
import os
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Any
import boto3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re
from dateutil.parser import parse
import isodate
from dotenv import load_dotenv
import csv

# 環境変数の読み込み
load_dotenv()

# S3クライアントの初期化
s3 = boto3.client('s3', region_name='ap-northeast-1')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME_GET_COMMENT')

# YouTube APIクライアントの初期化
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# ももクロの公式チャンネルID
CHANNEL_ID = 'UC6YNWTm6zuMFsjqd0PO3G-Q'  # ももいろクローバーZ Official Channel

def format_rfc3339(dt: datetime) -> str:
    """datetime オブジェクトをRFC3339形式の文字列に変換"""
    utc_dt = dt.astimezone(pytz.UTC)
    return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def parse_duration(duration: str) -> int:
    """ISO 8601形式の動画時間を秒数に変換"""
    return int(isodate.parse_duration(duration).total_seconds())

def analyze_title(title: str) -> Dict[str, Any]:
    """動画タイトルを分析"""
    keywords = {
        'ライブ': ['ライブ', 'LIVE', 'live'],
        'メンバー': ['百田', '玉井', '佐々木', '高城', '有安', '早見'],
        'イベント': ['イベント', '配信', 'フェス', 'ツアー'],
        'MV': ['MV', 'Music Video', 'ミュージックビデオ'],
        'ダイジェスト': ['ダイジェスト', 'digest', 'DIGEST'],
    }
    
    result = {category: any(kw.lower() in title.lower() for kw in words)
              for category, words in keywords.items()}
    
    return result

def analyze_video_data(video_data: Dict[str, Any]) -> Dict[str, Any]:
    """動画データを分析して追加情報を付与"""
    # 投稿日時をパース
    published_at = parse(video_data['published_at'])
    jst = pytz.timezone('Asia/Tokyo')
    published_at_jst = published_at.astimezone(jst)
    
    # 現在時刻との差分を計算
    current_time = datetime.now(pytz.UTC)
    days_since_published = (current_time - published_at).days or 1  # 0除算を防ぐ
    
    # 統計情報を数値に変換
    stats = {k: int(v) for k, v in video_data['statistics'].items() if v.isdigit()}
    
    # 分析データを追加
    analysis = {
        'published_info': {
            'year': published_at_jst.year,
            'month': published_at_jst.month,
            'day': published_at_jst.day,
            'hour': published_at_jst.hour,
            'weekday': published_at_jst.strftime('%A'),
            'time_of_day': 'morning' if 5 <= published_at_jst.hour < 12 else
                          'afternoon' if 12 <= published_at_jst.hour < 17 else
                          'evening' if 17 <= published_at_jst.hour < 22 else 'night'
        },
        'duration_seconds': parse_duration(video_data['duration']),
        'title_analysis': analyze_title(video_data['title']),
        'daily_average': {
            'views': int(stats.get('viewCount', 0)) / days_since_published,
            'likes': int(stats.get('likeCount', 0)) / days_since_published,
            'comments': int(stats.get('commentCount', 0)) / days_since_published
        },
        'total_engagement': int(stats.get('viewCount', 0)) + 
                          int(stats.get('likeCount', 0)) + 
                          int(stats.get('commentCount', 0))
    }
    
    video_data['analysis'] = analysis
    return video_data

def get_channel_videos_for_year(channel_id: str, year: int) -> List[Dict[str, Any]]:
    """指定した年のチャンネルの動画情報を取得"""
    try:
        # 東京タイムゾーン
        tz = pytz.timezone('Asia/Tokyo')
        # 年の開始と終了時刻を設定
        start_date = datetime(year, 1, 1, tzinfo=tz)
        end_date = datetime(year + 1, 1, 1, tzinfo=tz)
        
        # UTC時刻に変換
        start_date_utc = start_date.astimezone(pytz.UTC)
        end_date_utc = end_date.astimezone(pytz.UTC)
        
        videos = []
        page_token = None
        total_videos = 0
        
        while True:
            # 検索リクエストを実行
            search_params = {
                'channelId': channel_id,
                'part': 'id,snippet',
                'order': 'date',
                'publishedAfter': format_rfc3339(start_date_utc),
                'publishedBefore': format_rfc3339(end_date_utc),
                'type': 'video',
                'maxResults': 50
            }
            if page_token:
                search_params['pageToken'] = page_token
                
            search_response = youtube.search().list(**search_params).execute()

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
                        # 分析データを追加
                        video_data = analyze_video_data(video_data)
                        videos.append(video_data)
                        total_videos += 1
                        print(f"Processed video {total_videos}: {video_data['title']}")

            # 次のページがあれば続行
            page_token = search_response.get('nextPageToken')
            if not page_token:
                break

        print(f"Total videos fetched for {year}: {total_videos}")
        return videos

    except HttpError as e:
        print(f'An HTTP error {e.resp.status} occurred: {e.content}')
        return []

def get_video_info(video_id):
    """動画の情報を取得"""
    try:
        response = youtube.videos().list(
            part='snippet,statistics',
            id=video_id
        ).execute()

        if response['items']:
            video = response['items'][0]
            return {
                'videoId': video_id,
                'title': video['snippet']['title'],
                'publishedAt': video['snippet']['publishedAt'],
                'channelId': video['snippet']['channelId'],
                'channelTitle': video['snippet']['channelTitle'],
                'viewCount': video['statistics'].get('viewCount', '0'),
                'likeCount': video['statistics'].get('likeCount', '0'),
                'commentCount': video['statistics'].get('commentCount', '0')
            }
        return None
    except Exception as e:
        print(f"動画情報の取得中にエラーが発生しました: {e}")
        return None

def get_comments(video_id):
    """動画のコメントを取得"""
    comments = []
    next_page_token = None

    # 動画情報を取得
    video_info = get_video_info(video_id)
    if not video_info:
        print(f"動画ID {video_id} の情報を取得できませんでした")
        return []

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
                    'videoId': video_id,
                    'videoTitle': video_info['title'],
                    'videoPublishedAt': video_info['publishedAt'],
                    'videoChannelId': video_info['channelId'],
                    'videoChannelTitle': video_info['channelTitle'],
                    'videoViewCount': video_info['viewCount'],
                    'videoLikeCount': video_info['likeCount'],
                    'videoCommentCount': video_info['commentCount'],
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
    
    # JSON形式で保存
    data = {
        'video_id': video_id,
        'timestamp': timestamp,
        'comments': comments
    }

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"youtube_comments/{json_filename}",
            Body=json.dumps(data, ensure_ascii=False, indent=2)
        )
        print(f"コメントを {json_filename} としてS3に保存しました")
    except Exception as e:
        print(f"JSONのS3への保存中にエラーが発生しました: {e}")

def save_csv_to_s3(comments, video_id, timestamp):
    """コメントをCSV形式でS3に保存"""
    csv_filename = f"comments_{video_id}_{timestamp}.csv"
    
    try:
        # CSVデータの作成
        csv_buffer = []
        csv_buffer.append("videoId,videoTitle,videoPublishedAt,videoChannelId,videoChannelTitle,videoViewCount,videoLikeCount,videoCommentCount,author,publishedAt,likeCount,text\n")
        for comment in comments:
            # テキスト内のカンマと改行をエスケープ
            text = comment['text'].replace(',', '，').replace('\n', ' ')
            csv_buffer.append(
                f"{comment['videoId']},"
                f"{comment['videoTitle'].replace(',', '，')},"
                f"{comment['videoPublishedAt']},"
                f"{comment['videoChannelId']},"
                f"{comment['videoChannelTitle'].replace(',', '，')},"
                f"{comment['videoViewCount']},"
                f"{comment['videoLikeCount']},"
                f"{comment['videoCommentCount']},"
                f"{comment['author'].replace(',', '，')},"
                f"{comment['publishedAt']},"
                f"{comment['likeCount']},"
                f"{text}\n"
            )

        # S3にアップロード
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"youtube_comments/{csv_filename}",
            Body=''.join(csv_buffer)
        )
        print(f"コメントを {csv_filename} としてS3に保存しました")
    except Exception as e:
        print(f"CSVのS3への保存中にエラーが発生しました: {e}")

def save_to_s3(comments, video_id):
    """コメントをS3に保存（JSONとCSV形式）"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # JSON形式で保存
    save_json_to_s3(comments, video_id, timestamp)
    
    # CSV形式で保存
    save_csv_to_s3(comments, video_id, timestamp)

def lambda_handler(event, context):
    try:
        # イベントから動画IDを取得
        video_id = event.get('video_id')
        if not video_id:
            return {
                'statusCode': 400,
                'body': json.dumps('video_id is required')
            }

        print(f"動画ID: {video_id} のコメントを取得中...")

        # コメントを取得
        comments = get_comments(video_id)
        print(f"{len(comments)}件のコメントを取得しました")

        # S3に保存
        save_to_s3(comments, video_id)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'video_id': video_id,
                'comment_count': len(comments)
            })
        }

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error',
                'error': str(e)
            })
        } 