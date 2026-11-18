.PHONY: help aws-verify verify upload-data export-data cfn-validate cfn-deploy cfn-status cfn-delete

STACK_NAME ?= olist-v5-pipeline-stack
PROJECT_PREFIX ?= olist-v5
ENV_NAME ?= dev
AWS_REGION ?= us-east-1
ALERT_EMAIL ?=
CFN_TEMPLATE = infra/cloudformation/template.yaml

help:
	@echo "Các lệnh tiện ích cho dự án AWS Data Pipeline (V5 - FCJ Standard):"
	@echo "  make aws-verify            - Kiểm tra danh tính và quyền AWS CLI hiện tại"
	@echo "  make verify                - Chạy kiểm tra cú pháp Python, unit tests, cfn-lint và dbt parse (offline)"
	@echo "  make upload-data BUCKET=... - Tải các file CSV từ thư mục data/ lên S3"
	@echo "  make export-data           - Xuất các bảng marts từ Redshift ra file CSV (exports/)"
	@echo "  make cfn-validate          - Kiểm tra cú pháp template CloudFormation (infra/cloudformation/template.yaml)"
	@echo "  make cfn-deploy [ALERT_EMAIL=you@email.com] - Deploy/Update CloudFormation Stack V5 ($0 Free Tier mode)"
	@echo "  make cfn-status            - Xem trạng thái và Outputs của CloudFormation Stack V5"
	@echo "  make cfn-delete            - Dọn rỗng S3 bucket của Stack V5 và xóa sạch CloudFormation Stack ($0 Clean-up)"

aws-verify:
	aws sts get-caller-identity

verify:
	python3 -m py_compile dags/ecommerce_pipeline_dag.py dags/ecommerce_weekly_full_refresh_dag.py include/ingestion_guards.py include/sqs_parser.py scripts/export_to_csv.py scripts/empty_versioned_bucket.py tests/test_ingestion_guards.py tests/test_sqs_parser.py
	python3 -m unittest tests/test_ingestion_guards.py tests/test_sqs_parser.py -v
	@if command -v cfn-lint >/dev/null 2>&1; then \
		cfn-lint $(CFN_TEMPLATE); \
	fi
	@if [ -x "./venv/bin/dbt" ]; then \
		DBT_REDSHIFT_PASSWORD=local_verify_dummy ./venv/bin/dbt parse --project-dir dbt --profiles-dir dbt; \
	elif command -v dbt >/dev/null 2>&1; then \
		DBT_REDSHIFT_PASSWORD=local_verify_dummy dbt parse --project-dir dbt --profiles-dir dbt; \
	else \
		echo "ℹ️ Bỏ qua dbt parse (không tìm thấy dbt CLI trong môi trường hiện tại)."; \
	fi

cfn-validate:
	aws cloudformation validate-template --template-body file://$(CFN_TEMPLATE) --region $(AWS_REGION)

cfn-deploy:
	aws cloudformation deploy \
		--stack-name $(STACK_NAME) \
		--template-file $(CFN_TEMPLATE) \
		--capabilities CAPABILITY_NAMED_IAM \
		--region $(AWS_REGION) \
		--parameter-overrides \
			ProjectPrefix=$(PROJECT_PREFIX) \
			EnvironmentName=$(ENV_NAME) \
			AlertEmail="$(ALERT_EMAIL)" \
			DeployRedshift=false

cfn-status:
	aws cloudformation describe-stacks \
		--stack-name $(STACK_NAME) \
		--region $(AWS_REGION) \
		--query "Stacks[0].{StackStatus:StackStatus,Outputs:Outputs}" \
		--output table

cfn-delete:
	@ACCOUNT_ID=$$(aws sts get-caller-identity --query Account --output text); \
	BUCKET_NAME="$(PROJECT_PREFIX)-raw-data-$(ENV_NAME)-$${ACCOUNT_ID}-$(AWS_REGION)"; \
	echo "🧹 Đang dọn rỗng S3 bucket (bao gồm toàn bộ Versions & DeleteMarkers) s3://$${BUCKET_NAME}..."; \
	aws s3 rm "s3://$${BUCKET_NAME}" --recursive --region $(AWS_REGION) >/dev/null 2>&1 || true; \
	python3 scripts/empty_versioned_bucket.py "$${BUCKET_NAME}" "$(AWS_REGION)" || true; \
	echo "🗑️ Đang xóa CloudFormation stack $(STACK_NAME)..."; \
	aws cloudformation delete-stack --stack-name $(STACK_NAME) --region $(AWS_REGION); \
	echo "⏳ Đang chờ stack xóa hoàn tất (stack-delete-complete)..."; \
	aws cloudformation wait stack-delete-complete --stack-name $(STACK_NAME) --region $(AWS_REGION); \
	echo "✅ Đã xóa sạch 100% tài nguyên của stack $(STACK_NAME) ($0 AWS Billing)!"

upload-data:
	@chmod +x scripts/upload_to_s3.sh
	@./scripts/upload_to_s3.sh $(BUCKET)

export-data:
	@if [ -x "./venv/bin/python" ]; then \
		./venv/bin/python scripts/export_to_csv.py; \
	else \
		python3 scripts/export_to_csv.py; \
	fi
