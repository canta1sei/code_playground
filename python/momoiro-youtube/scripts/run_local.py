import os
import sys
import json
from datetime import datetime
import pytz
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートを追加
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

# .envファイルのパスを設定
env_path = os.path.join(project_root, 'config', '.env')
load_dotenv(env_path, override=True)

# Lambda関数をインポート
from src.lambda_function import lambda_handler

def main():
    # 引数の処理
    if len(sys.argv) > 1:
        try:
            start_year = int(sys.argv[1])
            if start_year < 2008:  # YouTubeの開始年より前は無効
                print("エラー: 開始年は2008年以降を指定してください")
                sys.exit(1)
        except ValueError:
            print("エラー: 開始年は数値で指定してください")
            sys.exit(1)
    else:
        # 引数なしの場合は現在の年をデフォルトとして使用
        start_year = datetime.now(pytz.timezone('Asia/Tokyo')).year
    
    # Lambda関数のイベントデータを作成
    event = {
        'start_year': start_year
    }
    
    # Lambda関数を実行
    result = lambda_handler(event, None)
    
    # 結果を表示
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main() 