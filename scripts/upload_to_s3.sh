#!/usr/bin/env bash
set -e

# Kiểm tra tham số truyền vào
BUCKET_NAME=$1

if [ -z "$BUCKET_NAME" ]; then
    echo "❌ Lỗi: Bạn chưa cung cấp tên S3 Bucket!"
    echo "👉 Cú pháp: ./scripts/upload_to_s3.sh <ten-s3-bucket>"
    exit 1
fi

echo "🚀 Bắt đầu đồng bộ dữ liệu Olist lên S3: s3://${BUCKET_NAME}/raw/ ..."

# Kiểm tra thư mục data có file csv nào không
COUNT=$(ls -1 data/*.csv 2>/dev/null | wc -l)
if [ "$COUNT" -eq 0 ]; then
    echo "❌ Không tìm thấy file .csv nào trong thư mục data/!"
    echo "👉 Hãy giải nén các file CSV của bộ Olist vào thư mục data/ rồi chạy lại nhé."
    exit 1
fi

echo "📦 Tìm thấy $COUNT file CSV trong thư mục data/. Đang tải lên S3..."

# Dùng lệnh sync để đẩy tất cả file CSV vào folder /raw/ trên S3
aws s3 sync data/ "s3://${BUCKET_NAME}/raw/" --exclude "*" --include "*.csv"

echo "✅ Đã tải toàn bộ $COUNT file CSV lên S3 thành công!"
echo "🔍 Danh sách file trên S3:"
aws s3 ls "s3://${BUCKET_NAME}/raw/"
