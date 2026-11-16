.PHONY: help aws-verify verify upload-data export-data

help:
	@echo "Các lệnh tiện ích cho dự án AWS Data Pipeline:"
	@echo "  make aws-verify            - Kiểm tra danh tính và quyền AWS CLI hiện tại"
	@echo "  make verify                - Chạy kiểm tra cú pháp Python, unit tests và dbt parse (offline)"
	@echo "  make upload-data BUCKET=... - Tải các file CSV từ thư mục data/ lên S3"
	@echo "  make export-data           - Xuất các bảng marts từ Redshift ra file CSV (exports/)"

aws-verify:
	aws sts get-caller-identity

verify:
	python3 -m py_compile dags/ecommerce_pipeline_dag.py dags/ecommerce_weekly_full_refresh_dag.py include/ingestion_guards.py include/sqs_parser.py scripts/export_to_csv.py tests/test_ingestion_guards.py tests/test_sqs_parser.py
	python3 -m unittest tests/test_ingestion_guards.py tests/test_sqs_parser.py -v
	@if [ -x "./venv/bin/dbt" ]; then \
		DBT_REDSHIFT_PASSWORD=local_verify_dummy ./venv/bin/dbt parse --project-dir dbt --profiles-dir dbt; \
	elif command -v dbt >/dev/null 2>&1; then \
		DBT_REDSHIFT_PASSWORD=local_verify_dummy dbt parse --project-dir dbt --profiles-dir dbt; \
	else \
		echo "ℹ️ Bỏ qua dbt parse (không tìm thấy dbt CLI trong môi trường hiện tại)."; \
	fi

upload-data:
	@chmod +x scripts/upload_to_s3.sh
	@./scripts/upload_to_s3.sh $(BUCKET)

export-data:
	@python3 scripts/export_to_csv.py
