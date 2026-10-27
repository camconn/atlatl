# Spike S8: Redshift Data API, UNLOAD + MANIFEST on Serverless

Date: 2026-09-28. No proof code: nothing here was run. Sources are the public AWS docs (links inline), the botocore model for `redshift-data` 2019-12-20, the AWS service-authorization JSON for `redshift-data` and `redshift-serverless`, and `aws-sdk-redshiftdata` 1.120.0 on docs.rs.

## Verdict

**Blocked.** Verifying this needs a **non-production AWS account** with a Redshift Serverless workgroup, a scratch S3 bucket and an UNLOAD IAM role (§3.0). The user forbids writes to production, and UNLOAD writes to S3, so nothing was attempted. The 06 §4 design holds on paper. The docs force five changes, which have been made to the spec (§5):
- a retry can't reuse an UNLOAD prefix, because UNLOAD fails if files already exist;
- `TIMESTAMPTZ` loses its zone in Parquet;
- a zero-row Parquet UNLOAD probably writes no data files;
- `CancelStatement` allows only 3 TPS per account;
- long polling (`WaitTimeSeconds`) replaces most `DescribeStatement` polling.

Items marked **(verify)** are unconfirmed until the runbook in §3 has been run.

## 1. Data API facts

Sources: [Using the Data API](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html), [ExecuteStatement](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_ExecuteStatement.html), [DescribeStatement](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_DescribeStatement.html), [CancelStatement](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_CancelStatement.html), [long polling](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-calling-considerations-long-polling.html), [quotas](https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html#data-api-quotas-account).

| Limit | Value |
|---|---|
| Max query duration | 24 h (Serverless: "timeout for a running query" 86,399 s, not adjustable) |
| Max active statements (`SUBMITTED` + `STARTED`) | 500 per cluster/workgroup |
| Max SQL text | 200 KB |
| Max result size | 500 MB after gzip; the call is ended past that |
| Result retention | 24 h. After that, `DescribeStatement`/`GetStatementResult` on the id fail |
| `ClientToken` retention | 8 h. The SDK generates one automatically and reuses it on its own retries, so a retried `ExecuteStatement` doesn't run an UNLOAD twice |
| `StatementName` | ≤ 2048 chars |
| `BatchExecuteStatement` | ≤ 40 statements, run serially; one transaction by default (`ExecutionMode: TRANSACTION`) |
| `WaitTimeSeconds` (long poll) | 1–30 s on `ExecuteStatement`, `BatchExecuteStatement`, `DescribeStatement` and `GetStatementResult(V2)`. It returns early on a terminal state |
| Sessions (`SessionKeepAliveSeconds`) | ≤ 24 h, ≤ 500 per workgroup, one query at a time. atlatl doesn't use them |
| `GetStatementResultV2` (CSV) | 1 MB chunks, ≤ 15 MB per page |

**TPS quotas per account and Region, not adjustable.** All pods share them:

| API | TPS |
|---|---|
| `ExecuteStatement` | 30 |
| `BatchExecuteStatement` | 20 |
| `DescribeStatement` | 100 |
| `GetStatementResult` | 20 |
| `CancelStatement` | **3** |
| `ListStatements`, `ListDatabases`, `ListSchemas`, `ListTables`, `DescribeTable` | 3 |

Going over a quota returns `ThrottlingException` (HTTP 400).

**Statuses.** `SUBMITTED` → `PICKED` → `STARTED` → one of `FINISHED | FAILED | ABORTED`. `ALL` is a filter value for `ListStatements` only. The response carries:
- `Error`, the message from the cluster;
- `Duration` in **nanoseconds**;
- `ResultRows`, which is `-1` when null;
- `ResultSize`, `RedshiftQueryId` (joins to `STL_QUERY`/`SYS_QUERY_HISTORY`), `RedshiftPid`, `HasResultSet` and `UpdatedAt`.

**Errors (botocore model):**

| Exception | HTTP | On | atlatl class |
|---|---|---|---|
| `ThrottlingException` | 400 | all | `throttled` |
| `ActiveStatementsExceededException` | 400 | Execute/Batch | `throttled` |
| `ActiveSessionsExceededException` | 400 | Execute/Batch | `throttled` |
| `ActiveWaitingRequestsExceededException` | 400 | Describe (long poll) | `throttled`; drop to plain polling |
| `ExecuteStatementException` / `BatchExecuteStatementException` ("environmental error") | 500 | Execute/Batch | `transient` |
| `DatabaseConnectionException` | 500 | Cancel | `transient` |
| `QueryTimeoutException` | 400 | Cancel | `transient` |
| `InternalServerException` | 500 | all | `transient` |
| `ValidationException` | 400 | all | `config` |
| `ResourceNotFoundException` | 400 | all | `internal`. For Get with long poll: "not ready yet" |
| `FAILED` status | — | Describe | classify on `Error` text: SQL/permission → `config`/`data`; capacity/connection → `transient` **(verify the texts)** |

**CancelStatement.** It returns `{"Status": true}` on success. The docs say "To be canceled, a query must be running". What cancelling a `SUBMITTED`/`PICKED` or already-finished statement does is **(verify)**. With a 3 TPS account quota, a kill that cancels many in-flight members must go through a rate limiter and must not retry storms.

**Authentication on Serverless.** There are three options:
- **Temporary credentials:** `WorkgroupName` + `Database`, no `DbUser`. The caller needs `redshift-serverless:GetCredentials`. The DB user is derived from the IAM identity: the docs show `IAM:foo` for a user, and a role is expected to map to `IAMR:<role-name>` **(verify)**. It is added to `PUBLIC` and has only the grants given to that name.
- **Secrets Manager:** `SecretArn` + `WorkgroupName` + `Database`. The caller needs `secretsmanager:GetSecretValue`.
- **IAM Identity Center:** not used.

`GetCredentials` passwords last 900–3600 s, but the Data API handles this internally.

## 2. UNLOAD facts

Sources: [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html), [UNLOAD examples](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD_command_examples.html), [permissions](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-access-permissions.html), [Serverless roles](https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-security-other-services.html).

- **Names.** With `TO 's3://b/p/part_'`:
  - with `PARALLEL ON`, files are `part_<slice:04>_part_<nn:02>.parquet` (e.g. `part_0000_part_00.parquet`), and the manifest is `part_manifest`;
  - with `PARALLEL OFF`, files are `part_0000.parquet`, …
- **Existing files.** By default UNLOAD **fails if it would overwrite files**:
  - `ALLOWOVERWRITE` overwrites;
  - `CLEANPATH` deletes the prefix first and needs `s3:DeleteObject`;
  - the two can't be combined.

- **Parquet.**
  - The format is Parquet 1.0 with SNAPPY per row group. There's no file-level compression.
  - It can't be combined with `DELIMITER, FIXEDWIDTH, ADDQUOTES, ESCAPE, NULL AS, HEADER, GZIP, BZIP2, ZSTD`.
  - `ENCRYPTED` works only with SSE-KMS (`KMS_KEY_ID`). The default is SSE-S3.
- **`MAXFILESIZE`.** The range is 5 MB–6.2 GB (the default is 6.2 GB). For Parquet it is **rounded down to a multiple of 32 MB** (the row-group size). `256 MB` is exactly 8 row groups, and the actual size is approximate. `ROWGROUPSIZE` (32–128 MB) works only on some provisioned node types.
- **`TIMESTAMPTZ` in Parquet:** "only the timestamp values are unloaded. The time zone information isn't unloaded." The values should be the stored UTC instants, written as naive timestamps **(verify)**. The physical types (INT96 vs INT64, and DECIMAL as INT32/INT64/FIXED_LEN_BYTE_ARRAY) aren't documented **(verify with `parquet_schema()`)**.
- **Unsupported in Parquet:** `VARBYTE`, `GEOMETRY` and `HLLSKETCH` unload only to text/CSV. `SUPER` is untested **(verify if any `est_v2` column uses it; none is expected)**.
- **Zero rows.** For CSV "Redshift might write empty objects". For JSON "does not write objects". Parquet isn't stated; aws-sdk-pandas [#2052](https://github.com/aws/aws-sdk-pandas/issues/2052) reports no files. Whether the manifest is written, and with `entries: []`, is **(verify)**.
- **`LIMIT`** isn't allowed in the outer SELECT. Nest it (`WHERE k IN (SELECT … LIMIT n)`), or create a temp table first.
- **Manifest.**
  - Plain `MANIFEST` gives `{"entries":[{"url":"s3://…"}]}`. The recent doc examples also show `"meta":{"content_length":N}` without VERBOSE.
  - `MANIFEST VERBOSE` adds, per entry, `meta.record_count` and `meta.content_length`. It also adds `schema.elements[{name,type{base,…}}]`, a top-level `meta{content_length,record_count}` and `author{name:"Amazon Redshift",version:"1.0.0"}`.
  - atlatl should use **VERBOSE**. It gives per-file row counts that the read-back is checked against, and it identifies empty files.
- **Authorisation.**
  - `IAM_ROLE { default | 'SESSION' | '<arn>[,<arn>…]' }`.
  - On Serverless the role must be **associated with the namespace**, and its trust policy must name both `redshift.amazonaws.com` and `redshift-serverless.amazonaws.com`.
  - A DB user may be limited to certain roles with `GRANT ASSUMEROLE ON '<arn>' TO <user> FOR UNLOAD`. By default all users can use the roles associated with the namespace **(verify on the workgroup)**.
- **Encryption and Region.** UNLOAD always uses HTTPS and SSE. `REGION` is needed only when the bucket is in another Region. S3 access-point aliases aren't supported.

### 2.1 IAM, exactly

From the service-authorization JSON:
- `ExecuteStatement` and `BatchExecuteStatement` are scoped to a `workgroup` resource, `arn:aws:redshift-serverless:<region>:<acct>:workgroup/<workgroup-id>` (a UUID, not the name).
- `DescribeStatement`, `GetStatementResult`, `CancelStatement` and `ListStatements` take **no resource**, so they need `"Resource": "*"`. They support the condition `redshift-data:statement-owner-iam-userid`.
- `GetStatementResultV2` is authorised by `redshift-data:GetStatementResult`.
- `redshift-serverless:GetCredentials` is scoped to the same `workgroup` ARN.

**Pod role** (atlatl caller):
```json
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["redshift-data:ExecuteStatement","redshift-data:BatchExecuteStatement"],
  "Resource":"arn:aws:redshift-serverless:us-east-1:<acct>:workgroup/<wg-id>"},
 {"Effect":"Allow","Action":["redshift-data:DescribeStatement","redshift-data:GetStatementResult",
  "redshift-data:CancelStatement","redshift-data:ListStatements"],"Resource":"*",
  "Condition":{"StringEquals":{"redshift-data:statement-owner-iam-userid":"${aws:userid}"}}},
 {"Effect":"Allow","Action":"redshift-serverless:GetCredentials",
  "Resource":"arn:aws:redshift-serverless:us-east-1:<acct>:workgroup/<wg-id>"},
 {"Effect":"Allow","Action":"sts:AssumeRole","Resource":"arn:aws:iam::<acct>:role/<read_role>"}
]}
```
Add `secretsmanager:GetSecretValue` on the secret for `auth: secret_arn`. For comparison, the AWS-managed `AmazonRedshiftDataFullAccess` (v5) grants the ten Data API actions on `*` and `GetCredentials` only on workgroups tagged `RedshiftDataFullAccess`.

**UNLOAD role** (assumed by Redshift):
- Trust: `redshift.amazonaws.com` and `redshift-serverless.amazonaws.com`.
- `s3:PutObject` and `s3:GetObject` on `arn:aws:s3:::<bucket>/atlatl_unload/*`.
- `s3:ListBucket` (condition `s3:prefix` = `atlatl_unload/*`) and `s3:GetBucketLocation` on `arn:aws:s3:::<bucket>`. The docs say "GET, LIST, and PUT".
- `s3:DeleteObject` only if `CLEANPATH`/`ALLOWOVERWRITE` is used. atlatl uses neither.
- For an SSE-KMS bucket or `KMS_KEY_ID`: `kms:GenerateDataKey` and `kms:Decrypt` on the key.

**Read-back role** (assumed by atlatl; today's `warehouse-unload` plays both roles):
- `s3:GetObject` and `s3:DeleteObject` on the prefix.
- `s3:ListBucket` for `atlatl gc`.
- `kms:Decrypt` if SSE-KMS is used.
- A trust policy for the pod role.

## 3. Runbook: what to run when a non-production account exists

### 3.0 Prerequisites

The following are set up by whoever owns the account; atlatl never creates them:
- a Serverless namespace and workgroup (base 8 RPU is enough), in the same Region as the bucket;
- a scratch bucket with a 1-day lifecycle rule on `atlatl_unload/`;
- the roles in §2.1, with the UNLOAD role associated with the namespace (`aws redshift-serverless update-namespace --iam-roles …`; the list **replaces** the existing roles, so include them all);
- a pod-like principal to run the CLI as.

Seed data:
```sql
CREATE SCHEMA s8;
CREATE TABLE s8.t (id bigint, n numeric(18,4), big numeric(38,10), small numeric(9,2), ts timestamp, tz timestamptz,
                   d date, t time, b boolean, s varchar(64), c char(3), f float8);
INSERT INTO s8.t VALUES (1, 1.2345, 12345678901234567890.1234567890, 1.23, '2024-01-31 12:34:56.123456',
                         '2024-01-31 12:34:56.123456+05', '2024-02-29', '23:59:59', true, 'it''s', 'abc', 1e-300),
                        (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO s8.t SELECT id + 2, n, big, small, ts, tz, d, t, b, s, c, f FROM s8.t;   -- repeat to ~5M rows
```

### 3.1 Checks

| # | Run | Expect / record |
|---|---|---|
| 1 | `aws redshift-data execute-statement --workgroup-name wg --database dev --sql "select current_user, version()" --wait-time-seconds 10` then `get-statement-result` | `Status FINISHED` in the first response. The `current_user` spelling for a role (`IAMR:<role>`?) |
| 2 | `execute-statement` with the UNLOAD of 06 §4 (`FORMAT PARQUET MANIFEST VERBOSE MAXFILESIZE 256 MB PARALLEL ON`, prefix `…/r1/s8/m/a1/part_`), then poll `describe-statement` without long poll every 500 ms | the status sequence (does `PICKED` appear?); `ResultRows` for UNLOAD (unloaded rows or `-1`?); `Duration` in ns; `HasResultSet=false` |
| 3 | `aws s3 ls` of the prefix, fetch `part_manifest` | the file names and manifest shape match §2; `sum(entries.meta.record_count)` = `meta.record_count` = `count(*)` |
| 4 | DuckDB 1.5.3 (httpfs + aws, scoped secret from the read role): `SELECT * FROM parquet_schema(<file>)`, then `read_parquet([...])` | the physical and logical types of each column; `numeric(38,10)` exact; `ts` µs precision; `tz` value (expect `07:34:56.123456` UTC, naive); `time`, `char(3)` padding, `NaN`/`1e-300` for float8 |
| 5 | Run #2 again with the **same** prefix | fails; record the exact `Error` text so it can be classified as `internal` (a prefix bug), not `transient` |
| 6 | UNLOAD `WHERE false` | are any data files written? Is the manifest written, with `entries: []` and `meta.record_count: 0`? |
| 7 | UNLOAD an ~5 GB selection with `PARALLEL ON`, then `cancel-statement` while `STARTED` | `Status: true`; `describe` → `ABORTED`; list leftover partial files under the prefix (expect some; gc must handle them) |
| 8 | `cancel-statement` on a `FINISHED` id, and on one just `SUBMITTED` (submit and cancel immediately) | the error type and message, or `Status:false` |
| 9 | Submit an UNLOAD with `--wait-time-seconds 30`, then two concurrent `describe-statement --wait-time-seconds 30` on the same id | when `ActiveWaitingRequestsExceededException` fires (how many concurrent waits are allowed) |
| 10 | 10 parallel `describe-statement` loops at ~15 TPS each | `ThrottlingException` near 100 TPS; that the SDK's standard retry absorbs it |
| 11 | Force `FAILED`: a bad column; a missing table; UNLOAD with a role not associated with the namespace; a bucket in another Region without `REGION` | the `Error` texts, for the classifier fixture |
| 12 | Run #2 as the pod principal after revoking `SELECT` on `s8.t` from `IAMR:<role>` | `FAILED` with a permission error → `config` |
| 13 | A statement older than 24 h: `describe-statement` | `ResourceNotFoundException` (confirms the retention limit) |
| 14 | UNLOAD selecting a `VARBYTE`/`SUPER` column as Parquet | the error text. Confirm no `est_v2` table has such columns (`svv_columns`) |
| 15 | Capture CloudTrail for #2 | IAM actions actually evaluated (to trim §2.1 to least privilege) |

Clean up by dropping `s8` and letting the lifecycle rule empty the prefix. Record the results as an addendum here, and turn the texts from #5, #8 and #11 into classifier test fixtures.

## 4. How M5 is tested until then

Spec 12 §2 already calls for a fake. Concretely:
- **An HTTP-level fake of the Data API.** `aws-sdk-redshiftdata` is pointed at a local server with `endpoint_url` and static credentials. The protocol is awsJson1.1: `POST /`, `X-Amz-Target: RedshiftData.<Op>`, `Content-Type: application/x-amz-json-1.1`. Errors are `{"__type": "<Exception>", "message": …}` with the HTTP status from §1.
  - This tests the real SDK path: serialization, the error types, and the SDK's retry and `ClientToken`.
  - The fake implements `ExecuteStatement`, `DescribeStatement` (with `WaitTimeSeconds`), `GetStatementResult` (paginated with `NextToken`) and `CancelStatement`. A per-test script drives the status sequence (`SUBMITTED` → `PICKED` → `STARTED` → `FINISHED|FAILED|ABORTED`) and injects any exception from §1.
- **SQL assertions.** Each test declares the expected statements (normalised whitespace, with the inner quotes of UNLOAD checked exactly), the result sets for `svv_columns`/`svv_tables`, and the `Error` text for failures.
- **UNLOAD emulation.** The fake parses the `TO` prefix and options. It then writes into a RustFS bucket (`atlatl_testkit::rustfs()`) using DuckDB `COPY … TO 's3://…' (FORMAT parquet)` from a test-supplied table:
  - Redshift-style names (`<prefix>0000_part_00.parquet` …, split into several files) and a `MANIFEST VERBOSE` JSON;
  - the documented quirks: `timestamptz` written as naive UTC, **no files** for zero rows (and a variant with an empty-entries manifest until #6 settles it), and failure when the prefix already has files.
- **Read-back** goes through the real path: STS is skipped (static RustFS keys in a scoped secret), then manifest → `read_parquet` → staging, with object-level fault injection (toxiproxy, a truncated object, a deleted object).
- **The wire fallback** runs against Postgres 16 as a stand-in for the SQL layer only (no UNLOAD).
- Real Redshift runs only in staging (12 §2), behind a flag, as the §3 runbook automated.

## 5. Spec changes

- 06 §4:
  - long polling via `WaitTimeSeconds`, with backoff only as a fallback;
  - the exact error mapping, the numeric limits and the TPS quotas, including a rate limiter on `CancelStatement` (3 TPS);
  - exact IAM resources and conditions;
  - an attempt-scoped UNLOAD prefix (UNLOAD refuses to overwrite);
  - `MANIFEST VERBOSE` with row-count validation;
  - `timestamptz` restored from `svv_columns` as UTC;
  - zero-row UNLOAD handled as an empty read, not an error.
- Open question 5 (read-back role) is unchanged. §2.1 lists what that role needs.
