.PHONY: help aws-verify upload-data

help:
	@echo "Các lệnh tiện ích cho dự án AWS Data Pipeline:"
	@echo "  make aws-verify            - Kiểm tra danh tính và quyền AWS CLI hiện tại"
	@echo "  make upload-data BUCKET=... - Tải các file CSV từ thư mục data/ lên S3"

aws-verify:
	aws sts get-caller-identity

upload-data:
	@chmod +x scripts/upload_to_s3.sh
	@./scripts/upload_to_s3.sh $(BUCKET)
