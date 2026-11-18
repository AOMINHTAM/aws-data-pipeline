# 📘 OPERATIONS, BACKUP, RESTORE & CLEAN-UP RUNBOOK (V5 — FCJ STANDARD)

> 🌐 **Language:** **English** | [Tiếng Việt](OPERATIONS_RUNBOOK.vi.md)

This runbook provides repeatable, copy-pasteable AWS CLI procedures for the 5 operational capabilities required by the **AWS First Cloud Journey (FCJ Workforce)** standard:
1. **Infrastructure Deployment, Change Sets & Automated Rollback (AWS CloudFormation)**
2. **Poison Message Isolation & SQS Dead-Letter Queue (DLQ) Redrive**
3. **Data Warehouse Backup & Restore (Redshift Serverless Recovery Points & Snapshots)**
4. **S3 Archive Event Replay**
5. **100% AWS Resource Clean-up (\$0.00 Billing Verification)**

---

## 1. Infrastructure Deployment, Change Sets & Rollback (`infra/cloudformation/template.yaml`)

### 1.1. Validate Template Syntax (Offline & AWS Engine)
```bash
# Static lint check (also executed automatically in GitHub Actions CI)
cfn-lint infra/cloudformation/template.yaml

# AWS CloudFormation engine validation
make cfn-validate
```

### 1.2. Preview Changes with a CloudFormation Change Set (Safe Dry-Run)
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

### 1.3. Deploy Stack (\$0 Free-Tier Mode — `DeployRedshift=false`)
```bash
make cfn-deploy ALERT_EMAIL="your-email@example.com"
make cfn-status
```
> ⚠️ **Post-Deploy Actions:**
> 1. **Confirm SNS Email:** Open your `ALERT_EMAIL` inbox and click **"Confirm subscription"** in the message from `AWS Notifications` so Amazon SNS can deliver alarm emails.
> 2. **Associate IAM Role with Existing Redshift Namespace (when `DeployRedshift=false`):**
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

### 1.4. Infrastructure Rollback Procedures
* **Native Automatic Rollback:** If any resource fails during creation or update, AWS CloudFormation automatically transitions to `ROLLBACK_IN_PROGRESS` / `UPDATE_ROLLBACK_IN_PROGRESS` and restores the stack to its previous clean state.
* **Manual Update Cancellation / Rollback Recovery:**
  ```bash
  # Cancel an in-progress stack update and roll back immediately
  aws cloudformation cancel-update-stack \
    --stack-name olist-v5-pipeline-stack \
    --region us-east-1

  # Resume a paused update rollback
  aws cloudformation continue-update-rollback \
    --stack-name olist-v5-pipeline-stack \
    --region us-east-1
  ```

---

## 2. Poison Message Handling with SQS Dead-Letter Queue (DLQ) & Redrive

### 2.1. How Poison Messages Are Isolated
The main ingestion queue (`*-s3-raw-arrival-queue-*`) enforces a `RedrivePolicy` with **`maxReceiveCount = 3`**:
* If a CSV is malformed or contains **0 data rows** (blocked by `ensure_non_empty_stage` in `include/ingestion_guards.py`), `load_raw_from_sqs` rolls back the Redshift transaction and fails.
* After **3 failed attempts**, Amazon SQS moves the message to **`*-s3-raw-arrival-dlq-*`**, stopping infinite retries.
* Within 60 seconds, **`DlqMessagesVisibleAlarm`** (`ApproximateNumberOfMessagesVisible >= 1`) transitions to `ALARM` and dispatches an email via **Amazon SNS**.

### 2.2. Inspect, Fix & Redrive Poison Messages
```bash
# 1. Inspect the failed event payload in the DLQ
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

# 2. Recover based on failure root cause (Avoiding Duplicate S3 Events):
#    - CASE A (Transient infra error — file in raw/ is already valid, no re-upload needed):
#      Simply run `start-message-move-task` below to redrive the original event from DLQ.
#    - CASE B (Corrupt/0-row CSV — requires uploading a corrected CSV to s3://<bucket>/raw/<filename>.csv):
#      Uploading a new object to raw/ automatically emits a fresh s3:ObjectCreated:* event to SqsMainQueue.
#      Either purge the obsolete poison message from the DLQ (`aws sqs purge-queue --queue-url "$DLQ_URL"`),
#      OR pause the DAG before uploading + running `start-message-move-task` so both handles coalesce
#      cleanly under the Two-Proof Safe ACK Guard.

# 3. Trigger SQS DLQ Redrive back to the main queue
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

## 3. Data Warehouse Backup & Restore (Redshift Serverless Recovery Points & Snapshots)

> 💡 **Naming Note:** Set `REDSHIFT_NAMESPACE` / `REDSHIFT_WORKGROUP` to your active Redshift resources (`ecommerce-namespace` / `ecommerce-workgroup` when reusing V4 with `DeployRedshift=false`, or `${ProjectPrefix}-namespace-${EnvironmentName}` / `${ProjectPrefix}-workgroup-${EnvironmentName}` when provisioned via `DeployRedshift=true`).

* **Automatic Recovery Points (RPO = 30 min):** Redshift Serverless automatically creates recovery points every 30 minutes and retains them for 24 hours.
* **Create a Manual Snapshot Before Major Changes:**
  ```bash
  REDSHIFT_NAMESPACE="${REDSHIFT_NAMESPACE:-ecommerce-namespace}"
  REDSHIFT_WORKGROUP="${REDSHIFT_WORKGROUP:-ecommerce-workgroup}"

  aws redshift-serverless create-snapshot \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --snapshot-name "olist-pre-upgrade-$(date +%Y%m%d-%H%M)" \
    --retention-period 7 \
    --region us-east-1
  ```
* **Restore from a Recovery Point:**
  ```bash
  aws redshift-serverless list-recovery-points \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --region us-east-1 \
    --query "recoveryPoints[0:5].{Id:recoveryPointId,Time:recoveryPointCreateTime}" \
    --output table

  aws redshift-serverless restore-from-recovery-point \
    --recovery-point-id "<RECOVERY_POINT_ID>" \
    --namespace-name "$REDSHIFT_NAMESPACE" \
    --workgroup-name "$REDSHIFT_WORKGROUP" \
    --region us-east-1
  ```

---

## 4. S3 Archive Event Replay

To re-process any historical batch without re-uploading from a local machine (using the deterministic `archive/<safe_audit_id>/<filename>` path recorded in `ops.pipeline_ingestion_audit`):
```bash
aws s3 ls s3://$S3_BUCKET_NAME/archive/

# Copy back from archive/<safe_audit_id>/ to raw/ -> triggers s3:ObjectCreated:Copy -> SQS -> Airflow 3
aws s3 cp \
  s3://$S3_BUCKET_NAME/archive/ver_abc123__product_category_name_translation/product_category_name_translation.csv \
  s3://$S3_BUCKET_NAME/raw/product_category_name_translation.csv
```

---

## 5. 100% AWS Resource Clean-up Guide (\$0.00 Billing Verification)

1. **Stop local Airflow Triggerer polling:**
   ```bash
   astro dev stop
   ```
2. **Empty S3 Bucket & Delete CloudFormation Stack in one command:**
   ```bash
   make cfn-delete
   ```
3. **(If deleting manual Redshift Serverless resources) Remove Workgroup & Namespace:**
   ```bash
   aws redshift-serverless delete-workgroup --workgroup-name ecommerce-workgroup --region us-east-1
   aws redshift-serverless delete-namespace --namespace-name ecommerce-namespace --region us-east-1
   ```
4. **Verify zero remaining stacks:**
   ```bash
   aws cloudformation list-stacks \
     --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE DELETE_FAILED \
     --region us-east-1 \
     --query "StackSummaries[].{Name:StackName,Status:StackStatus}" \
     --output table
   ```

---

## 6. Querying the Ingestion Audit Ledger (`ops.pipeline_ingestion_audit`)

Every run of `ecommerce_aws_data_pipeline` records per-table lifecycle transitions (`SWAPPED` / `COMPLETED` / `FAILED`) and verified S3 `archive_key` paths in `ops.pipeline_ingestion_audit`:

```sql
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

## 7. V5 Critical Reliability & Security Test Matrix (14 Scenarios)

| # | Scenario | Priority | Action & Expected Outcome |
| :-: | :--- | :---: | :--- |
| 1 | **Happy Path E2E** | P0 | Upload valid CSV to `raw/` → S3 → SQS → Airflow `AssetWatcher` → Atomic Swap (`SWAPPED`) → dbt PASS → Deterministic Archive (`ARCHIVED` → `COMPLETED`) → SQS ACK. |
| 2 | **Empty CSV (0 data rows)** | P0 | Upload header-only CSV → `ensure_non_empty_stage` raises `ValueError`, Redshift `rollback()` preserves target table, audit logs `FAILED`, message retries/routes to DLQ. |
| 3 | **COPY Failure (Malformed CSV)** | P0 | Upload corrupted CSV → `COPY` to `TEMP TABLE` fails → `rollback()` preserves target table, audit logs `FAILED`. |
| 4 | **dbt Test Failure** | P0 | Simulated dbt test failure blocks Task 3 (`archive_and_ack`), keeping raw object on S3 and message un-ACKed. |
| 5 | **Archive Copy Verification** | P0 | `check_for_key(dest_key)` verified after `copy_object` and `ARCHIVED` recorded before deleting `raw/`. |
| 6 | **Safe ACK Guard (Missing Raw)** | P0 | When `raw/` is missing on step 3 retry, `verify_safe_to_ack_when_raw_missing` enforces `status in {'ARCHIVED','COMPLETED'}` AND matching deterministic `archive_key`. |
| 7 | **Audit Insert Resilience** | P0 | Audit errors are caught safely and premature ACK is blocked if `ARCHIVED`/`COMPLETED` proof is absent. |
| 8 | **SQS ACK Retry Safety** | P0 | Failed `DeleteMessage` calls return `failed_handles` and raise `RuntimeError` so Airflow retries Task 3 independently. |
| 9 | **Visibility Timeout Heartbeat** | P0 | `_refresh_sqs_visibility` calls `change_message_visibility(900s)` post-Redshift swap and pre-archive/ACK. |
| 10 | **Triggerer Restart** | P0 | `delete_message_on_reception=False` preserves messages on SQS across Triggerer restarts. |
| 11 | **Duplicate Event Idempotency** | P1 | Duplicate deliveries for already archived objects skip Redshift wakeup (\$0 RPU) at Task 1 start. |
| 12 | **DLQ Routing & Redrive** | P0 | After `maxReceiveCount=3`, poison messages route to `SqsDeadLetterQueue` (`RedriveAllowPolicy: byQueue`); fix + `start-message-move-task` replays cleanly. |
| 13 | **4-Alarm & SNS TopicPolicy** | P1 | 4 CloudWatch Alarms (`DLQ >= 1`, `OldestAge > 600s`, `InFlight >= 1 for 15m`, `Backlog >= 20`) publish via `PipelineAlertTopicPolicy` to email. |
| 14 | **Redshift Network & 1-Cmd Clean-up** | P1 | Conditional Redshift enforces `PubliclyAccessible=false`, `/32` CIDR, wired `SecurityGroupIds`/`SubnetIds`; `make cfn-delete` purges S3 Versions + Stack. |
