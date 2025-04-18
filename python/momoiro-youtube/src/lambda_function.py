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

# S3クライアントの初期化
s3 = boto3.client('s3')
BUCKET_NAME = os.environ['S3_BUCKET_NAME']

# YouTube APIクライアントの初期化
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']
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
                
                # S3に保存するファイル名の生成（直下に配置）
                file_name = f"youtube_videos/videos_{year}_{len(videos)}.json"
                
                # JSONデータをS3にアップロード
                data = {
                    'metadata': metadata,
                    'videos': videos
                }
                
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_name,
                    Body=json.dumps(data, ensure_ascii=False, indent=2),
                    ContentType='application/json'
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