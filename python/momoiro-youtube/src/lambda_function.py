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
import csv
import io

# S3クライアントの初期化
s3 = boto3.client('s3', region_name='ap-northeast-1')
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
                    video_id = item['id']['videoId']
                    print(f"Processing video ID: {video_id}")
                    
                    try:
                        # 動画の詳細情報を取得
                        video_response = youtube.videos().list(
                            part='statistics,contentDetails',
                            id=video_id
                        ).execute()

                        if not video_response['items']:
                            print(f"Warning: No video details found for {video_id}")
                            continue

                        video_stats = video_response['items'][0]
                        print(f"Video stats: {json.dumps(video_stats, ensure_ascii=False)}")
                        
                        # 各フィールドの存在チェックとデフォルト値設定
                        content_details = video_stats.get('contentDetails', {})
                        statistics = video_stats.get('statistics', {})
                        
                        # durationのデフォルト値設定
                        duration = content_details.get('duration', 'PT0S')
                        
                        # 統計情報のデフォルト値設定
                        view_count = statistics.get('viewCount', '0')
                        like_count = statistics.get('likeCount', '0')
                        comment_count = statistics.get('commentCount', '0')
                        
                        video_data = {
                            'video_id': video_id,
                            'title': item['snippet'].get('title', ''),
                            'description': item['snippet'].get('description', ''),
                            'published_at': item['snippet'].get('publishedAt', ''),
                            'thumbnail_url': item['snippet'].get('thumbnails', {}).get('high', {}).get('url', ''),
                            'channel_id': channel_id,
                            'channel_title': item['snippet'].get('channelTitle', ''),
                            'statistics': {
                                'viewCount': view_count,
                                'likeCount': like_count,
                                'commentCount': comment_count
                            },
                            'duration': duration,
                            'fetched_at': datetime.now(pytz.UTC).isoformat(),
                            'status': 'SUCCESS'  # 取得成功
                        }
                        
                        # 分析データを追加
                        try:
                            video_data = analyze_video_data(video_data)
                            videos.append(video_data)
                            total_videos += 1
                            print(f"Processed video {total_videos}: {video_data['title']}")
                        except Exception as e:
                            print(f"Warning: Analysis failed for video {video_id}: {str(e)}")
                            video_data['status'] = 'ANALYSIS_ERROR'  # 分析失敗
                            videos.append(video_data)
                            total_videos += 1
                            
                    except Exception as e:
                        print(f"Warning: Failed to process video {video_id}: {str(e)}")
                        # エラー情報を含む最小限のデータを保存
                        error_data = {
                            'video_id': video_id,
                            'title': item['snippet'].get('title', ''),
                            'status': 'FETCH_ERROR',  # 取得失敗
                            'error_message': str(e),
                            'fetched_at': datetime.now(pytz.UTC).isoformat()
                        }
                        videos.append(error_data)
                        total_videos += 1

            # 次のページがあれば続行
            page_token = search_response.get('nextPageToken')
            if not page_token:
                break

        print(f"Total videos fetched for {year}: {total_videos}")
        return videos

    except HttpError as e:
        print(f'An HTTP error {e.resp.status} occurred: {e.content}')
        return []

def convert_to_csv_row(video_data: Dict[str, Any]) -> Dict[str, Any]:
    """動画データをCSV行用のフラット形式に変換"""
    # 現在の日本時間を取得
    jst = pytz.timezone('Asia/Tokyo')
    current_time_jst = datetime.now(jst).strftime('%Y-%m-%d %H:%M:%S')
    
    # 基本情報
    row = {
        'video_id': video_data.get('video_id', ''),
        'title': video_data.get('title', ''),
        'status': video_data.get('status', 'UNKNOWN'),
        'recorded_at': current_time_jst
    }
    
    # エラーが発生した場合は、エラーメッセージを追加
    if video_data.get('status') == 'FETCH_ERROR':
        row['error_message'] = video_data.get('error_message', '')
        return row
    
    # 正常に取得できた場合の追加情報
    stats = video_data.get('statistics', {})
    analysis = video_data.get('analysis', {})
    
    # 分析情報が存在する場合のみ追加
    if analysis:
        published_info = analysis.get('published_info', {})
        row.update({
            'published_at': video_data.get('published_at', ''),
            'year': published_info.get('year', ''),
            'month': published_info.get('month', ''),
            'day': published_info.get('day', ''),
            'hour': published_info.get('hour', ''),
            'weekday': published_info.get('weekday', ''),
            'time_of_day': published_info.get('time_of_day', ''),
            'duration_seconds': analysis.get('duration_seconds', 0),
            'view_count': stats.get('viewCount', '0'),
            'like_count': stats.get('likeCount', '0'),
            'comment_count': stats.get('commentCount', '0'),
            'daily_avg_views': round(analysis.get('daily_average', {}).get('views', 0), 2),
            'daily_avg_likes': round(analysis.get('daily_average', {}).get('likes', 0), 2),
            'daily_avg_comments': round(analysis.get('daily_average', {}).get('comments', 0), 2),
            'total_engagement': analysis.get('total_engagement', 0),
            'is_live': analysis.get('title_analysis', {}).get('ライブ', False),
            'is_mv': analysis.get('title_analysis', {}).get('MV', False),
            'is_digest': analysis.get('title_analysis', {}).get('ダイジェスト', False),
            'is_event': analysis.get('title_analysis', {}).get('イベント', False),
            'has_member_name': analysis.get('title_analysis', {}).get('メンバー', False)
        })
    
    return row

def save_to_csv(videos: List[Dict[str, Any]], s3_client: boto3.client, bucket: str, csv_key: str) -> None:
    """動画データをCSVとしてS3に保存"""
    if not videos:
        print("No videos provided to save_to_csv")
        return
        
    print(f"Preparing to save {len(videos)} videos to CSV")
    
    # CSVヘッダーの準備
    csv_row = convert_to_csv_row(videos[0])
    fieldnames = list(csv_row.keys())
    
    # メモリ上でCSVを作成
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    
    try:
        print("Checking for existing CSV file...")
        existing_content = s3_client.get_object(Bucket=bucket, Key=csv_key)['Body'].read().decode('utf-8')
        print("Found existing CSV file, appending data...")
        # 既存のデータを保持
        output.write(existing_content.rstrip())
        if not existing_content.endswith('\n'):
            output.write('\n')
    except s3_client.exceptions.NoSuchKey:
        print("No existing CSV file found, creating new one...")
        # ファイルが存在しない場合はヘッダーを書き込む
        writer.writeheader()
    
    # 新しいデータを追記
    print("Writing video data to CSV...")
    for video in videos:
        writer.writerow(convert_to_csv_row(video))
    
    # S3にアップロード
    print("Uploading CSV to S3...")
    s3_client.put_object(
        Bucket=bucket,
        Key=csv_key,
        Body=output.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )
    output.close()
    print("CSV upload completed")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        # 東京タイムゾーン
        tz = pytz.timezone('Asia/Tokyo')
        # 実行時の現在時刻
        current_time = datetime.now(tz)
        
        # 実行日付のフォルダ名を生成（yyyy=YYYY/mm=MM/dd=DD形式）
        date_folder = f"yyyy={current_time.year}/mm={current_time.month:02d}/dd={current_time.day:02d}"
        
        # 取得対象の年を設定
        end_year = current_time.year
        start_year = event.get('start_year', current_time.year)  # イベントから開始年を取得、デフォルトは現在の年
        
        print(f"Fetching videos from {start_year} to {end_year}")
        print(f"Data will be saved in folder: {date_folder}")
        
        all_videos = []
        
        # 各年の動画を取得
        for year in range(start_year, end_year + 1):
            print(f"\nFetching videos for year {year}")
            videos = get_channel_videos_for_year(CHANNEL_ID, year)
            all_videos.extend(videos)
            print(f"Added {len(videos)} videos for year {year}")
            
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
                
                # JSONファイルとして保存（日付フォルダ配下に配置）
                json_key = f'{date_folder}/video_stats_{year}.json'
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=json_key,
                    Body=json.dumps({
                        'metadata': metadata,
                        'videos': videos
                    }, ensure_ascii=False, indent=2),
                    ContentType='application/json'
                )
                
                print(f"Saved JSON data for {year} to S3: {json_key}")
        
        print(f"\nTotal videos collected: {len(all_videos)}")
        
        # 全年のデータをCSVとして保存（日付フォルダ配下に配置）
        if all_videos:
            print("Attempting to save CSV file...")
            csv_key = f'{date_folder}/video_stats.csv'
            save_to_csv(all_videos, s3, BUCKET_NAME, csv_key)
            print("CSV file saved successfully")
        else:
            print("No videos to save to CSV")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed video data',
                'years_processed': list(range(start_year, end_year + 1)),
                'total_videos': len(all_videos),
                'date_folder': date_folder
            })
        }
        
    except Exception as e:
        print(f'Error: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 