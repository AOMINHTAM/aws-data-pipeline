# 🔍 V5 ARCHITECTURE DEEP DIVE — CLOUDFORMATION IaC, FAULT TOLERANCE & CLOUDOPS

> 🌐 **Language:** **English** | [Tiếng Việt](V5_DEEP_DIVE.vi.md)
>
> This document explains the **engineering rationale**, **under-the-hood AWS & Airflow 3 mechanics**, and **architectural trade-offs** introduced in **V5 (`5.0.0`)** — bridging the V4 Event-Driven Data Core into a production-ready **AWS First Cloud Journey (FCJ)** CloudOps system.

---

## 1. Why Upgrade from V4 (`v4.0.1`) to V5 (`5.0.0`)?

By **V4 (`v4.0.1`)**, the Data Engineering core was complete (zero-cron Airflow 3 `AssetWatcher`, atomic `TEMP TABLE` swap + Empty-Stage Guard, 4-source incremental `fct_orders`, and 3-job GitHub Actions CI). However, five operational gaps remained under CloudOps & FCJ standards:

1. **No Infrastructure as Code (IaC):** AWS resources (S3, SQS, IAM, Redshift) were provisioned manually — lacking single-command reproducibility and automated deployment rollback.
2. **No Poison Message Isolation:** A malformed or 0-row CSV blocked by `ensure_non_empty_stage` would exceed `VisibilityTimeout` and loop indefinitely in `SqsMainQueue`.
3. **No Proactive Alerting:** Queue backlogs, hung consumers, or repeated failures produced no CloudWatch Alarms or email notifications.
4. **Coarse IAM & Missing Consumer Separation:** Runtime Airflow permissions, Redshift `COPY` permissions, and DLQ redrive permissions were not separated under Least Privilege.
5. **Retry-Path Blind Spot:** When `raw/` was already empty on a Step 3 retry, V4 assumed archival had succeeded and ACKed SQS without checking durable ledger proof.

**V5 (`5.0.0`)** closes all five gaps cleanly without introducing unnecessary infrastructure (no Kafka, Spark, Lambda, or DynamoDB overhead).

---

## 2. CloudFormation IaC Anatomy (`infra/cloudformation/template.yaml`)

[`infra/cloudformation/template.yaml`](../infra/cloudformation/template.yaml) provisions **15 core resources** (including `S3RawBucketPolicy` enforcing HTTPS-only transport), **4 conditional FinOps/VPC Redshift resources**, and **16 stack Outputs**.

### 2.1. Breaking Dual Circular Dependencies (S3 ↔ SQS Main Queue ↔ DLQ)
1. **S3 Bucket ↔ SQS Queue Policy:** `S3RawBucket` validates its `NotificationConfiguration` immediately upon creation (sending `s3:TestEvent`), so `SqsMainQueuePolicy` must exist first (`DependsOn: SqsMainQueuePolicy`). To prevent a circular reference, `SqsMainQueuePolicy` constructs the bucket ARN with `!Sub` instead of `!GetAtt S3RawBucket.Arn`.
2. **SQS Main Queue ↔ SQS DLQ:** `SqsMainQueue.RedrivePolicy` references `!GetAtt SqsDeadLetterQueue.Arn`. To add `RedriveAllowPolicy` (`byQueue`) on `SqsDeadLetterQueue` without creating a reverse cycle, the DLQ constructs the main queue ARN via `!Sub`.

### 2.2. Versioned S3 Storage, Lifecycle & Safe Stack Tear-Down
- **Security & Naming:** Enables `AES256` SSE, all four `PublicAccessBlockConfiguration` flags, `S3RawBucketPolicy` (`Deny` when `aws:SecureTransport: "false"`), and constrains `ProjectPrefix` to `3–16` chars (`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`) so the generated bucket name always stays well under AWS's 63-character limit.
- **Versioning & Lifecycle:** Enables `VersioningConfiguration: Enabled`, expires `archive/` after 90 days, expires noncurrent object versions after 30 days, purges orphan delete markers via `ExpiredObjectDeleteMarker: true`, and cleans up incomplete multipart uploads after 7 days (`AbortIncompleteMultipartUpload`).
- **Preventing `DELETE_FAILED` on Versioned Buckets:** Standard `aws s3 rm --recursive` only writes delete markers on versioned buckets. [`scripts/empty_versioned_bucket.py`](../scripts/empty_versioned_bucket.py) loops across all paginated `list-object-versions` responses to purge all `Versions` and `DeleteMarkers` in batches of 500 before `make cfn-delete` calls `aws cloudformation delete-stack`.

### 2.3. Split Least-Privilege IAM (3 Distinct Region-Scoped Roles/Policies)
- **`RedshiftS3ReadOnlyRole` (`*-${AWS::Region}`):** STS `AssumeRole` restricted by `aws:SourceAccount`, granting `s3:ListBucket` (`s3:prefix: ["raw/*", "raw"]`) and `s3:GetObject`/`GetObjectVersion` strictly on `raw/*` (cannot read `archive/`).
- **`AirflowRuntimePolicy` (`*-${AWS::Region}`):** Optionally attached to `AirflowPrincipalUserName` (`Default: ""` for fresh-account portability); grants SQS consume/ACK/visibility actions strictly on `SqsMainQueue`, `GET/DELETE` on `raw/*`, and `GET/PUT` (**no `DELETE`**) on `archive/*`.
- **`DlqOperatorPolicy` (`*-${AWS::Region}`):** Optionally attached to a separate `DlqOperatorUserName` (`Default: ""`, enforcing principal-level Separation of Duties); isolates DLQ inspection and `StartMessageMoveTask` / `CancelMessageMoveTask` / `ListMessageMoveTasks` + `SendMessage` back to `SqsMainQueue`.

### 2.4. Four CloudWatch Alarms & `PipelineAlertTopicPolicy`
All four alarms set `TreatMissingData: notBreaching` and publish both `AlarmActions` and `OKActions` through `PipelineAlertTopicPolicy` (which restricts `sns:Publish` to `cloudwatch.amazonaws.com` scoped by `aws:SourceAccount` and `aws:SourceArn`):

| Alarm | Metric & Threshold | Operational Purpose |
| :--- | :--- | :--- |
| **`DlqMessagesVisibleAlarm`** | `ApproximateNumberOfMessagesVisible >= 1` (`60s`, `1/1`) | Fires **CRITICAL** within 1 minute whenever a poison CSV isolates in the DLQ. |
| **`MainQueueOldestMessageAlarm`** | `ApproximateAgeOfOldestMessage > 600s` (`60s`, `1/1`) | Enforces a 10-minute SLA distinct from the 900s `VisibilityTimeout`. |
| **`MainQueueInFlightAlarm`** | `ApproximateNumberOfMessagesNotVisible >= 1` (`60s`, `Minimum`, `15/15`) | Surfaces sustained queue-level in-flight activity (15 consecutive minutes); note that continuous overlapping traffic can also trip this metric. |
| **`MainQueueBacklogAlarm`** | `ApproximateNumberOfMessagesVisible >= 20` (`60s`, `Average`, `3/3`) | Detects sustained message accumulation across 3 consecutive minutes. |

### 2.6. FinOps Toggle (`DeployRedshift`) & Hardened Conditional Redshift
- **`DeployRedshift=false` (default):** Deploys and tests the 14 core S3/SQS/DLQ/Alarm/SNS/IAM resources while reusing an existing Redshift Serverless workgroup.
- **`DeployRedshift=true`:** Provisions a VPC-scoped workgroup (`PubliclyAccessible=false` by default, `SecurityGroupIds`, `SubnetIds`, `/32` CIDR validation) and automatically enables **`ManageAdminPassword: true`** (AWS Secrets Manager) when `RedshiftAdminPassword` is omitted.

---

## 3. Application Reliability: Airflow 3 Bugfix, Audit Ledger & Safe ACK

### 3.1. Real Airflow 3 Task SDK Bug Caught Live
During live Bước 5 testing on Airflow 3.3, `extract_messages_from_asset_events` initially returned `[]` and skipped the run even though `SqsSensorTrigger` emitted a valid `TriggerEvent`:
- **Root Cause:** In Airflow 3 Task SDK, `context["triggering_asset_events"]` is a `TriggeringAssetEventsAccessor` (subclass of `collections.abc.Mapping`, **not `dict`**), so `isinstance(..., dict)` returned `False`.
- **Fix:** Updated [`include/sqs_parser.py`](../include/sqs_parser.py) to check `collections.abc.Mapping` and added dedicated unit tests in [`tests/test_sqs_parser.py`](../tests/test_sqs_parser.py) and [`tests/test_ingestion_guards.py`](../tests/test_ingestion_guards.py) (**20/20 unit tests passing**).

### 3.2. `ops.pipeline_ingestion_audit`, Two-Phase Batch Staging, Deterministic Archive & Safe ACK
1. **Two-Phase Batch Staging, CSV Header Guard & Same-Transaction `SWAPPED` Audit:** [`dags/ecommerce_pipeline_dag.py`](../dags/ecommerce_pipeline_dag.py) first validates the CSV header row against [`EXPECTED_CSV_HEADERS`](../include/ingestion_guards.py) (`validate_csv_header`), stages every file in the batch into `TEMP TABLE tmp_stage_<table>`, and verifies `ensure_non_empty_stage` (`> 0` rows) **before** touching any target table in `raw_data.*`. Once all files in the batch pass staging, Phase B executes `DELETE + INSERT` and records `SWAPPED` (`rows_loaded = staged_rows`, fail-closed) **inside the same Redshift transaction**.
2. **Cross-Run Event Identity & Deterministic Archive Keys:** [`parse_sqs_s3_events`](../include/sqs_parser.py) enforces the strict flat key contract `raw/<allowlisted_filename>.csv` (blocking subfolder collisions and SQL injection) and extracts an immutable `event_token` (`versionId` / `eTag` / `sequencer` / `MessageId`). [`build_audit_id`](../include/ingestion_guards.py) derives `audit_id` (capped at $\le 256$ chars) from `event_token` so [`build_deterministic_archive_key`](../include/ingestion_guards.py) (`archive/<safe_audit_id>/<filename>`) remains identical even if SQS redelivers a message to a brand-new DAG Run.
3. **Two-Proof Safe ACK Guard (Task 1 & Task 3) & Deferred Multi-Record ACK:** Both Task 1 (`load_raw_from_sqs`) and Task 3 (`archive_and_ack_sqs_messages`) enforce [`verify_safe_to_ack_when_raw_missing`](../include/ingestion_guards.py) whenever `raw/<file>` is absent — permitting SQS ACK **only if** the latest audit row (ordered deterministically with a status-rank tie-breaker) is `ARCHIVED` or `COMPLETED` **and** the deterministic `archive_key` exists on S3. Task 3 defers `_delete_sqs_handles` until **all** files in the batch have completed archival and audit writes, protecting multi-record SQS messages from premature ACK.
4. **Fail-Closed Visibility Refresh Heartbeat:** `_refresh_sqs_visibility` extends `VisibilityTimeout` to `900s` after staging/swapping and enforces `fail_on_error=True` prior to S3 archival/deletion so a consumer that lost its SQS lease aborts immediately.

---

## 4. FinOps Breakdown & Transparent Trade-offs

- **Always Free Allowances (even on Paid Tier):** CloudFormation (`$0`), IAM (`$0`), SQS (`1M` reqs/month free — 24/7 Triggerer polling uses ~`104k`/month; `0` when `astro dev stop`), CloudWatch (`4/10` free standard alarms), SNS (`1,000` emails/month free).
- **Paid Storage/Compute:** S3 storage (`~120 MB` $\approx$ `$0.003/month`), Redshift Serverless (`$0` compute while asleep; bills RPU-seconds only during active queries + `$0.024/GB/month` storage).
- **Known Scope Boundaries:** `parse_sqs_s3_events` groups by `filename` under a single-snapshot-per-table batch contract, and `load_raw_from_sqs` commits per target table (if a multi-file batch fails mid-way, un-ACKed messages can share retry coupling). Local dev user `de-ecommerce-dev` retains broader V2–V4 policies for running CloudFormation deployments alongside the attached least-privilege V5 runtime policies.
