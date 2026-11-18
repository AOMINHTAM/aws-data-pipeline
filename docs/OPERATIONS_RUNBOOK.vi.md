# 📘 SỔ TAY VẬN HÀNH, SAO LƯU, KHÔI PHỤC & DỌN DẸP HẠ TẦNG (OPERATIONS & DISASTER RECOVERY RUNBOOK — V5 FCJ STANDARD)

> 🌐 **Ngôn ngữ / Language:** [English](OPERATIONS_RUNBOOK.md) | **Tiếng Việt**

Tài liệu này cung cấp quy trình thực chiến từng bước (kèm lệnh AWS CLI copy-paste) cho 5 kịch bản vận hành bắt buộc của chuẩn **AWS First Cloud Journey (FCJ Workforce)**:
1. **Triển khai & Hoàn tác Hạ tầng tự động (CloudFormation Deploy, Change Sets & Rollback)**
2. **Xử lý Gói tin Lỗi với SQS Dead-Letter Queue (DLQ) & DLQ Redrive**
3. **Sao lưu & Khôi phục Kho dữ liệu (Redshift Serverless Recovery Points & Snapshots)**
4. **Phát lại Sự kiện Dữ liệu từ S3 Archive (S3 Event Replay)**
5. **Dọn dẹp 100% Tài nguyên AWS về \$0.00 (100% Clean-up Verification)**

---

## 1. Triển khai & Hoàn tác Hạ tầng bằng AWS CloudFormation (`infra/cloudformation/template.yaml`)

### 1.1. Kiểm tra cú pháp Template trước khi Deploy (Static & Engine Validation)
```bash
# Kiểm tra offline bằng cfn-lint (chạy tự động trong GitHub Actions CI)
cfn-lint infra/cloudformation/template.yaml

# Kiểm tra trực tiếp với AWS CloudFormation Engine
make cfn-validate
```

### 1.2. Xem trước thay đổi bằng CloudFormation Change Set (Dry-Run an toàn)
Trước khi áp dụng thay đổi lên môi trường thật, tạo **Change Set** để xem chính xác AWS sẽ thêm (`Add`), sửa (`Modify`) hay xóa (`Remove`) tài nguyên nào:
```bash
aws cloudformation deploy \
  --stack-name olist-v5-pipeline-stack \
  --template-file infra/cloudformation/template.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-execute-changeset \
  --region us-east-1 \
  --parameter-overrides \
    ProjectPrefix=olist-v5 \
    EnvironmentName=dev \
    AlertEmail="your-email@example.com" \
    DeployRedshift=false
```

### 1.3. Thực thi Deploy Stack (Chế độ \$0 Free Tier — `DeployRedshift=false`)
```bash
make cfn-deploy ALERT_EMAIL="your-email@example.com"
make cfn-status
```
> ⚠️ **2 Bước quan trọng ngay sau khi Deploy:**
> 1. **Xác nhận Email SNS:** Mở hộp thư `ALERT_EMAIL` của bạn và bấm **"Confirm subscription"** trong email gửi từ `AWS Notifications` để kích hoạt nhận cảnh báo từ Amazon SNS.
> 2. **Gắn IAM Role mới vào Redshift Namespace hiện có (khi `DeployRedshift=false`):**
>    ```bash
>    ROLE_ARN=$(aws cloudformation describe-stacks \
>      --stack-name olist-v5-pipeline-stack \
>      --region us-east-1 \
>      --query "Stacks[0].Outputs[?OutputKey=='RedshiftS3ReadOnlyRoleArn'].OutputValue" \
>      --output text)
>
>    aws redshift-serverless update-namespace \
>      --namespace-name "${REDSHIFT_NAMESPACE:-ecommerce-namespace}" \
>      --iam-roles "$ROLE_ARN" \
>      --default-iam-role-arn "$ROLE_ARN" \
>      --region us-east-1
>    ```
>    *(Đồng thời cập nhật lại `REDSHIFT_IAM_ROLE=$ROLE_ARN` trong `.env` bất cứ khi nào Role ARN mới được tạo hoặc thay thế sau khi update stack).*

### 1.4. Cơ chế Rollback Hạ tầng (Infrastructure Rollback)
* **Tự động Rollback khi Deploy lỗi (Native Auto-Rollback):**
  * Mặc định, nếu bất kỳ tài nguyên nào trong `template.yaml` bị lỗi khi đang khởi tạo hoặc cập nhật (ví dụ trùng tên bucket hoặc thiếu quyền IAM), AWS CloudFormation tự động chuyển sang trạng thái `ROLLBACK_IN_PROGRESS` → thu hồi toàn bộ các tài nguyên vừa tạo dở dang → đưa stack về `ROLLBACK_COMPLETE` hoặc phiên bản ổn định liền trước (`UPDATE_ROLLBACK_COMPLETE`).
* **Chủ động Rollback về phiên bản trước hoặc tiếp tục Rollback bị kẹt:**
  ```bash
  # Hủy ngay một lượt update đang chạy dở để quay về trạng thái cũ
  aws cloudformation cancel-update-stack \
    --stack-name olist-v5-pipeline-stack \
    --region us-east-1

  # Nếu một lần rollback trước bị tạm dừng, ép tiếp tục rollback
  aws cloudformation continue-update-rollback \
    --stack-name olist-v5-pipeline-stack \
    --region us-east-1
  ```

---

## 2. Xử lý Gói tin Lỗi (Poison Message) với SQS Dead-Letter Queue (DLQ) & Redrive

### 2.1. Khi nào một message bị đẩy vào DLQ?
Hàng đợi chính `*-s3-raw-arrival-queue-*` được cấu hình `RedrivePolicy` với **`maxReceiveCount = 3`**:
* Nếu một file CSV bị hỏng định dạng hoặc **rỗng 0 dòng** (kích hoạt `ensure_non_empty_stage` trong `include/ingestion_guards.py`), task `load_raw_from_sqs` sẽ rollback giao dịch Redshift và báo lỗi.
* Sau **3 lần nhận và xử lý thất bại**, Amazon SQS tự động chuyển gói tin sự kiện đó sang hàng đợi cách ly **`*-s3-raw-arrival-dlq-*`**.
* Trong vòng 1 phút, **CloudWatch Alarm (`DlqMessagesVisibleAlarm`)** phát hiện `ApproximateNumberOfMessagesVisible >= 1` và bắn email cảnh báo qua **Amazon SNS**.

### 2.2. Quy trình 3 bước xử lý và Redrive từ DLQ
1. **Bước 1 — Đọc nội dung gói tin lỗi trong DLQ để tìm tên file CSV gây lỗi:**
   ```bash
   DLQ_URL=$(aws cloudformation describe-stacks \
     --stack-name olist-v5-pipeline-stack \
     --region us-east-1 \
     --query "Stacks[0].Outputs[?OutputKey=='SqsDeadLetterQueueUrl'].OutputValue" \
     --output text)

   aws sqs receive-message \
     --queue-url "$DLQ_URL" \
     --max-number-of-messages 5 \
     --visibility-timeout 30 \
     --region us-east-1
   ```
2. **Bước 2 — Khắc phục theo đúng nguyên nhân gốc (tránh sinh 2 sự kiện S3 trùng lặp):**
   * **Trường hợp A (Lỗi hạ tầng tạm thời — file trên `raw/` đã hợp lệ, không cần upload lại):** Chuyển thẳng sang **Bước 3** để chạy `start-message-move-task` đẩy message cũ từ DLQ về `SqsMainQueue`.
   * **Trường hợp B (File CSV rỗng `0` dòng hoặc sai định dạng — cần upload bản CSV mới lên `s3://<bucket>/raw/<filename>.csv`):** Bản thân thao tác upload file mới lên `raw/` sẽ tự động bắn 1 sự kiện `s3:ObjectCreated:*` mới vào `SqsMainQueue`. Khi đó bạn chỉ cần xóa message hỏng cũ trong DLQ (`aws sqs purge-queue --queue-url "$DLQ_URL"`), **hoặc** tạm pause DAG trước khi upload + chạy `start-message-move-task` rồi unpause để cơ chế **Two-Proof Safe ACK Guard** gộp xử lý idempotent.
3. **Bước 3 — Kích hoạt SQS DLQ Redrive (`StartMessageMoveTask`) để đẩy message ngược về hàng đợi chính:**
   ```bash
   DLQ_ARN=$(aws cloudformation describe-stacks \
     --stack-name olist-v5-pipeline-stack \
     --region us-east-1 \
     --query "Stacks[0].Outputs[?OutputKey=='SqsDeadLetterQueueArn'].OutputValue" \
     --output text)

   MAIN_QUEUE_ARN=$(aws cloudformation describe-stacks \
     --stack-name olist-v5-pipeline-stack \
     --region us-east-1 \
     --query "Stacks[0].Outputs[?OutputKey=='SqsMainQueueArn'].OutputValue" \
     --output text)

   aws sqs start-message-move-task \
     --source-arn "$DLQ_ARN" \
     --destination-arn "$MAIN_QUEUE_ARN" \
     --region us-east-1
   ```

---

## 3. Sao lưu & Khôi phục Kho dữ liệu (Redshift Serverless Recovery Points & Snapshots)

> 💡 **Lưu ý tên tài nguyên:** Đặt biến `REDSHIFT_NAMESPACE` / `REDSHIFT_WORKGROUP` theo workgroup đang dùng (`ecommerce-namespace` / `ecommerce-workgroup` khi tái sử dụng từ V4 với `DeployRedshift=false`, hoặc `${ProjectPrefix}-namespace-${EnvironmentName}` / `${ProjectPrefix}-workgroup-${EnvironmentName}` khi tạo mới bằng `DeployRedshift=true`).

* **RPO (Recovery Point Objective):** Amazon Redshift Serverless tự động tạo **Recovery Points** mỗi **30 phút** (lưu giữ trong 24 giờ hoàn toàn miễn phí compute).
* **Tạo Snapshot thủ công trước khi bảo trì lớn:**
  ```bash
  REDSHIFT_NAMESPACE="${REDSHIFT_NAMESPACE:-ecommerce-namespace}"
  REDSHIFT_WORKGROUP="${REDSHIFT_WORKGROUP:-ecommerce-workgroup}"

  aws redshift-serverless create-snapshot \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --snapshot-name "olist-pre-upgrade-$(date +%Y%m%d-%H%M)" \
    --retention-period 7 \
    --region us-east-1
  ```
* **Khôi phục Namespace hoặc từng bảng từ Recovery Point khi có sự cố dữ liệu:**
  ```bash
  # 1. Liệt kê các Recovery Points gần nhất trong 24 giờ qua
  aws redshift-serverless list-recovery-points \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --region us-east-1 \
    --query "recoveryPoints[0:5].{Id:recoveryPointId,Time:recoveryPointCreateTime}" \
    --output table

  # 2. Khôi phục toàn bộ Namespace về đúng mốc Recovery Point đã chọn
  aws redshift-serverless restore-from-recovery-point \
    --recovery-point-id "<RECOVERY_POINT_ID>" \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --workgroup-name "$REDSHIFT_WORKGROUP" \
    --region us-east-1
  ```

---

## 4. Phát lại Sự kiện Dữ liệu từ S3 Archive (S3 Event Replay)

Khi cần chạy lại một lô dữ liệu lịch sử mà không cần upload lại từ máy cá nhân (sử dụng đường dẫn deterministic `archive/<safe_audit_id>/<filename>` được ghi trong bảng `ops.pipeline_ingestion_audit`):
```bash
# Xem danh sách các batch đã archive trên S3
aws s3 ls s3://$S3_BUCKET_NAME/archive/

# Copy ngược 1 file (hoặc cả batch) từ archive/<safe_audit_id>/ về raw/
# S3 sẽ tự động phát sự kiện s3:ObjectCreated:Copy -> SQS -> đánh thức Airflow 3 AssetWatcher!
aws s3 cp \
  s3://$S3_BUCKET_NAME/archive/ver_abc123__product_category_name_translation/product_category_name_translation.csv \
  s3://$S3_BUCKET_NAME/raw/product_category_name_translation.csv
```

---

## 5. Quy trình Dọn dẹp 100% Tài nguyên AWS về \$0.00 (Clean-up Guide)

> 🚨 **BẮT BUỘC KHI KẾT THÚC LAB / WORKSHOP:** Thực hiện đúng thứ tự dưới đây để tránh lỗi `DELETE_FAILED` (do S3 bucket còn chứa file) và đảm bảo hóa đơn AWS về **\$0.00**.

### Bước 1 — Tắt Airflow Triggerer trên máy local
```bash
astro dev stop
```

### Bước 2 — Làm rỗng S3 Bucket & Xóa toàn bộ CloudFormation Stack bằng 1 lệnh
```bash
make cfn-delete
```
*(Lệnh `make cfn-delete` tự động chạy `aws s3 rm s3://<bucket> --recursive` và `python3 scripts/empty_versioned_bucket.py` để xóa sạch toàn bộ `Versions` + `DeleteMarkers`, sau đó gọi `aws cloudformation delete-stack` và chờ tới khi `stack-delete-complete`.)*

### Bước 3 — (Nếu tạo Redshift Serverless thủ công ngoài Stack) Xóa Workgroup, Namespace & Snapshots
```bash
# 1. Xóa Workgroup trước
aws redshift-serverless delete-workgroup \
  --workgroup-name ecommerce-workgroup \
  --region us-east-1

# 2. Sau khi Workgroup xóa xong, xóa Namespace (không giữ final snapshot để không tốn phí lưu trữ)
aws redshift-serverless delete-namespace \
  --namespace-name ecommerce-namespace \
  --region us-east-1
```

### Bước 4 — Kiểm tra xác nhận không còn tài nguyên tồn đọng
```bash
aws cloudformation list-stacks \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE DELETE_FAILED \
  --region us-east-1 \
  --query "StackSummaries[].{Name:StackName,Status:StackStatus}" \
  --output table
```

---

## 6. Truy vấn Đối soát Nhật ký Vận hành (`ops.pipeline_ingestion_audit`)

Mỗi lượt chạy của DAG `ecommerce_aws_data_pipeline` tự động ghi nhận trạng thái nạp (`SWAPPED` / `COMPLETED` / `FAILED`) và đường dẫn `archive_key` vào bảng `ops.pipeline_ingestion_audit` trên Redshift:

```sql
-- Kiểm tra 20 lượt xử lý file gần nhất và trạng thái đối soát
SELECT
    recorded_at,
    audit_id,
    table_name,
    rows_loaded,
    status,
    s3_key,
    archive_key,
    error_message
FROM ops.pipeline_ingestion_audit
ORDER BY recorded_at DESC
LIMIT 20;
```

---

## 7. Checklist Kiểm thử Độ tin cậy Hệ thống V5 (14 Critical Test Matrix)

| # | Kịch bản kiểm thử | Mức độ | Hành động & Kết quả kỳ vọng |
| :-: | :--- | :---: | :--- |
| 1 | **Happy Path E2E** | P0 | Upload CSV hợp lệ vào `raw/` → S3 → SQS → Airflow `AssetWatcher` → Atomic Swap (`SWAPPED`) → dbt PASS → Archive deterministic (`ARCHIVED` → `COMPLETED`) → SQS ACK. |
| 2 | **Empty CSV (0 dòng)** | P0 | Upload CSV chỉ có header → `ensure_non_empty_stage` raise `ValueError`, Redshift `rollback()` giữ nguyên bảng đích, ghi audit `FAILED`, message retry/vào DLQ. |
| 3 | **COPY Failure (Malformed CSV)** | P0 | Upload CSV lỗi cột → `COPY` vào `TEMP TABLE` lỗi → `rollback()` an toàn bảng đích, ghi audit `FAILED`. |
| 4 | **dbt Test Failure** | P0 | Giả lập lỗi dbt test → Task 3 (`archive_and_ack`) không chạy, file giữ ở `raw/`, message chưa ACK. |
| 5 | **Archive Copy Verification** | P0 | Kiểm tra `check_for_key(dest_key)` sau `copy_object` trước khi ghi `ARCHIVED` và xóa `raw/`. |
| 6 | **Safe ACK Guard (Missing Raw)** | P0 | Nếu `raw/` mất khi retry bước 3, `verify_safe_to_ack_when_raw_missing` bắt buộc `status in {'ARCHIVED','COMPLETED'}` VÀ đúng deterministic `archive_key` tồn tại. |
| 7 | **Audit Insert Resilience** | P0 | Lỗi ghi audit được bắt trong `_record_audit_event` (best-effort log) và chặn ACK nhầm nếu thiếu `ARCHIVED`/`COMPLETED`. |
| 8 | **SQS ACK Retry Safety** | P0 | Nếu `DeleteMessage` lỗi mạng, `_delete_sqs_handles` trả về `failed_handles` và raise `RuntimeError` để retry riêng Task 3. |
| 9 | **Visibility Timeout Heartbeat** | P0 | `_refresh_sqs_visibility` gọi `change_message_visibility(900s)` ngay sau Redshift swap và trước bước archive/ACK. |
| 10 | **Triggerer Restart** | P0 | `delete_message_on_reception=False` giữ message an toàn trên SQS khi Triggerer khởi động lại. |
| 11 | **Duplicate Event Idempotency** | P1 | Event gửi trùng cho file đã xử lý khỏi `raw/` được skip ngay đầu Task 1 (\$0 Redshift RPU) và ACK. |
| 12 | **DLQ Routing & Redrive** | P0 | Sau `maxReceiveCount=3`, message sang `SqsDeadLetterQueue` (`RedriveAllowPolicy: byQueue`); sửa file + `start-message-move-task` xử lý lại thành công. |
| 13 | **4-Alarm & SNS TopicPolicy** | P1 | 4 CloudWatch Alarms (`DLQ >= 1`, `OldestAge > 600s`, `InFlight >= 1 for 15m`, `Backlog >= 20`) publish qua `PipelineAlertTopicPolicy` về Gmail. |
| 14 | **Redshift Network & 1-Cmd Clean-up** | P1 | Conditional Redshift khóa `PubliclyAccessible=false`, `AuthorizedCidrIp` `/32`, nối `SecurityGroupIds`/`SubnetIds`; `make cfn-delete` xóa sạch S3 Versions + Stack. |
