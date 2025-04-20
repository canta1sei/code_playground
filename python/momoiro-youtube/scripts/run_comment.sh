#!/bin/bash

# 引数の確認
if [ $# -ne 1 ]; then
    echo "使用方法: $0 <動画ID>"
    echo "例: $0 R-H5R38Jym0"
    exit 1
fi

VIDEO_ID=$1

# 動画IDから余分なパラメータを削除（?以降を削除）
VIDEO_ID=$(echo $VIDEO_ID | cut -d'?' -f1)

# Pythonスクリプトの実行
python3 "$(dirname "$0")/run_local_comment.py" "$VIDEO_ID" 