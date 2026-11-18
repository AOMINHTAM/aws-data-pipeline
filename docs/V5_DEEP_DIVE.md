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

[`infra/cloudformation/template.yaml`](../infra/cloudformation/template.yaml) provisions **14 core resources**, **4 conditional FinOps/VPC Redshift resources**, and **16 stack Outputs**.

### 2.1. Breaking Dual Circular Dependencies (S3 ↔ SQS Main Queue ↔ DLQ)
1. **S3 Bucket ↔ SQS Queue Policy:** `S3RawBucket` validates its `NotificationConfiguration` immediately upon creation (sending `s3:TestEvent`), so `SqsMainQueuePolicy` must exist first (`DependsOn: SqsMainQueuePolicy`). To prevent a circular reference, `SqsMainQueuePolicy` constructs the bucket ARN with `!Sub` instead of `!GetAtt S3RawBucket.Arn`.
2. **SQS Main Queue ↔ SQS DLQ:** `SqsMainQueue.RedrivePolicy` references `!GetAtt SqsDeadLetterQueue.Arn`. To add `RedriveAllowPolicy` (`byQueue`) on `SqsDeadLetterQueue` without creating a reverse cycle, the DLQ constructs the main queue ARN via `!Sub`.

### 2.2. Versioned S3 Storage, Lifecycle & Safe Stack Tear-Down
- **Security & Naming:** Enables `AES256` SSE, all four `PublicAccessBlockConfiguration` flags, and constrains `ProjectPrefix` to `3–16` chars (`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`) so the generated bucket name always stays well under AWS's 63-character limit.
- **Versioning & Lifecycle:** Enables `VersioningConfiguration: Enabled`, expires `archive/` after 90 days, and expires noncurrent object versions after 30 days.
- **Preventing `DELETE_FAILED` on Versioned Buckets:** Standard `aws s3 rm --recursive` only writes delete markers on versioned buckets. [`scripts/empty_versioned_bucket.py`](../scripts/empty_versioned_bucket.py) removes all `Versions` and `DeleteMarkers` in batches of 500 before `make cfn-delete` calls `aws cloudformation delete-stack`.

### 2.3. Split Least-Privilege IAM (3 Distinct Roles/Policies)
- **`RedshiftS3ReadOnlyRole`:** STS `AssumeRole` restricted by `aws:SourceAccount`, granting `s3:ListBucket` (`s3:prefix: ["raw/*", "raw"]`) and `s3:GetObject`/`GetObjectVersion` strictly on `raw/*` (cannot read `archive/`).
- **`AirflowRuntimePolicy`:** Attached to `AirflowPrincipalUserName`; grants SQS consume/ACK/visibility actions strictly on `SqsMainQueue`, `GET/DELETE` on `raw/*`, and `GET/PUT` (**no `DELETE`**) on `archive/*`.
- **`DlqOperatorPolicy`:** Isolates DLQ inspection and `StartMessageMoveTask` / `CancelMessageMoveTask` / `ListMessageMoveTasks` + `SendMessage` back to `SqsMainQueue`.

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
- **Fix:** Updated [`include/sqs_parser.py`](../include/sqs_parser.py) to check `collections.abc.Mapping` and added a dedicated unit test in [`tests/test_sqs_parser.py`](../tests/test_sqs_parser.py) (**15/15 unit tests passing**).

### 3.2. `ops.pipeline_ingestion_audit`, Deterministic Archive & Safe ACK
1. **Accurate `rows_loaded` Accounting:** [`dags/ecommerce_pipeline_dag.py`](../dags/ecommerce_pipeline_dag.py) records `SWAPPED` (`rows_loaded = staged_rows`) **only after** `DELETE + INSERT` commits on `raw_data.<table>`; any `COPY` failure or `Empty-Stage Guard` rejection rolls back and logs `FAILED` (`rows_loaded = 0`). *(Redshift does not enforce physical `UNIQUE` constraints, so the audit table serves strictly as a verification ledger, not a distributed concurrency lock).*
2. **Deterministic Archive Keys:** [`build_deterministic_archive_key`](../include/ingestion_guards.py) maps every run's file to `archive/<safe_audit_id>/<filename>`, preventing duplicate timestamped archive folders across retries.
3. **Two-Proof Safe ACK Guard:** `archive_and_ack_sqs_messages` records `ARCHIVED` immediately after verifying `dest_key` exists in S3 and **before** deleting `raw/`. On retry when `raw/` is absent, [`verify_safe_to_ack_when_raw_missing`](../include/ingestion_guards.py) allows SQS ACK **only if** the latest audit row is `ARCHIVED` or `COMPLETED` **and** the exact deterministic `archive_key` exists on S3.
4. **Visibility Refresh Heartbeat:** `_refresh_sqs_visibility` extends `VisibilityTimeout` to `900s` immediately after Redshift swap and prior to archival/ACK.

---

## 4. FinOps Breakdown & Transparent Trade-offs

- **Always Free Allowances (even on Paid Tier):** CloudFormation (`$0`), IAM (`$0`), SQS (`1M` reqs/month free — 24/7 Triggerer polling uses ~`104k`/month; `0` when `astro dev stop`), CloudWatch (`4/10` free standard alarms), SNS (`1,000` emails/month free).
- **Paid Storage/Compute:** S3 storage (`~120 MB` $\approx$ `$0.003/month`), Redshift Serverless (`$0` compute while asleep; bills RPU-seconds only during active queries + `$0.024/GB/month` storage).
- **Known Scope Boundaries:** `parse_sqs_s3_events` groups by `filename` under a single-snapshot-per-table batch contract, and `load_raw_from_sqs` commits per target table (if a multi-file batch fails mid-way, un-ACKed messages can share retry coupling). Local dev user `de-ecommerce-dev` retains broader V2–V4 policies for running CloudFormation deployments alongside the attached least-privilege V5 runtime policies.
