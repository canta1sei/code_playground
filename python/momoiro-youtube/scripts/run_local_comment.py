import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートを追加
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

# .envファイルのパスを設定
env_path = os.path.join(project_root, 'config', '.env')
load_dotenv(env_path, override=True)

# コメント収集スクリプトをインポート
from src.youtube_comment_collector import main

# コメント収集スクリプトの実行
if __name__ == "__main__":
    # コマンドライン引数を設定
    import argparse
    parser = argparse.ArgumentParser(description='YouTube動画のコメントを取得してS3に保存します')
    parser.add_argument('video_id', help='YouTube動画ID')
    args = parser.parse_args()

    # コメント収集スクリプトを実行
    main(args.video_id) 