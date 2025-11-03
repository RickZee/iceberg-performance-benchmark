# The Journey to Success: Fixing AWS Glue Iceberg Integration with Snowflake

*A detective story of troubleshooting, discovery, and ultimate resolution*

## The Problem: When Everything Should Work, But Doesn't

It started with a simple goal: enable Snowflake to query Iceberg tables managed by AWS Glue. The infrastructure was in place—24 TPC-DS tables created in Glue, all with proper Iceberg metadata files in S3. The catalog integration was configured. The IAM roles were set up. Everything *should* have worked.

But it didn't.

### The First Error: UnknownOperationException

The initial error was cryptic and frustrating:

```
SQL Execution Error: Failed to access the REST endpoint of catalog integration 
AWS_GLUE_CATALOG with error: Unable to process: <UnknownOperationException/>
```

This suggested that Snowflake couldn't communicate with AWS Glue's REST API. The catalog integration was using `CATALOG_SOURCE = ICEBERG_REST`, which relies on Glue's REST catalog API endpoints. But something was wrong with the connection.

**What we tried:**
- Verified IAM permissions
- Checked trust policies
- Ensured Glue tables existed
- Verified metadata files were in S3

**What didn't work:**
- The REST API approach was fundamentally incompatible with how Glue works
- AWS Glue doesn't fully implement the Iceberg REST catalog specification that Snowflake expects

## The Discovery: A Working Example

The breakthrough came when we discovered the `simple-glue-setup` folder—a reference implementation that actually worked. This was our Rosetta Stone.

### Key Differences Found

By carefully analyzing the working example, we discovered three critical differences:

1. **Catalog Source**: The working setup used `CATALOG_SOURCE = 'GLUE'` (native integration), not `ICEBERG_REST`
2. **Table Reference Method**: Used `CATALOG_TABLE_NAME` parameter instead of `METADATA_LOCATION`
3. **External Volume Path**: The `STORAGE_BASE_URL` exactly matched the S3 path where metadata files were located

### The First Fix: Switching to Native GLUE

We updated the catalog integration to use Snowflake's native Glue connector:

```sql
CREATE OR REPLACE CATALOG INTEGRATION AWS_GLUE_CATALOG
  CATALOG_SOURCE = 'GLUE'  -- Changed from ICEBERG_REST
  CATALOG_NAMESPACE = 'iceberg_performance_test'
  GLUE_CATALOG_ID = '978300727880'
  GLUE_REGION = 'us-east-1'
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::978300727880:role/snowflake-iceberg-role'
  TABLE_FORMAT = 'ICEBERG'
  ENABLED = TRUE;
```

**Result**: The `UnknownOperationException` disappeared! But we weren't done yet.

## The Second Challenge: Path Mismatches

After switching to native GLUE, we encountered a new error:

```
SQL Compilation error: One of the specified Iceberg metadata files does not conform 
to the required directory hierarchy. Current base directory: s3://iceberg-snowflake-perf/tcp-ds-data/. 
Conflicting file path: s3://iceberg-snowflake-perf/iceberg-snowflake-perf/iceberg_glue_format/...
```

The external volume was pointing to the wrong S3 path. Snowflake was looking for metadata files in `tcp-ds-data/`, but the actual files were in `iceberg-snowflake-perf/iceberg_glue_format/`.

### The Fix: Matching Paths

We updated the external volume to point to the correct location:

```sql
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_GLUE_VOLUME
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_storage'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://iceberg-snowflake-perf/iceberg-snowflake-perf/iceberg_glue_format/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::978300727880:role/snowflake-iceberg-role'
    )
  )
  ALLOW_WRITES = FALSE  -- Bypass test file creation for read-only access
```

**Result**: Directory hierarchy errors vanished. But the IAM trust policy issue remained.

## The Third Challenge: Test File Creation Failures

Next, we encountered:

```
A test file creation on the external volume ICEBERG_GLUE_VOLUME failed with 
the message 'Error assuming AWS_ROLE: User: arn:aws:iam::333861696097:user/ysoz0000-s 
is not authorized to perform: sts:AssumeRole'
```

Snowflake was trying to write a test file to verify the external volume, but the IAM trust policy didn't allow it. We set `ALLOW_WRITES = FALSE` to bypass this, which moved us forward.

**Result**: Table creation started working, but we still couldn't read the metadata files.

## The Final Mystery: The Case of the Two External IDs

The most subtle and critical issue emerged when we tried to read the actual Iceberg metadata files:

```
Failed to read from Iceberg file 's3://...': Error assuming AWS_ROLE:
User: arn:aws:iam::333861696097:user/ysoz0000-s is not authorized to perform: sts:AssumeRole
```

The IAM user ARN was correct. The trust policy looked correct. But it still wasn't working.

### The Discovery: Parsing the Hidden Details

The breakthrough came when we parsed the `DESC EXTERNAL VOLUME` output more carefully. The IAM details weren't in obvious columns—they were buried in a JSON field:

```sql
DESC EXTERNAL VOLUME ICEBERG_GLUE_VOLUME;
```

Looking at the `STORAGE_LOCATION_1` row, we found:
- `STORAGE_AWS_IAM_USER_ARN`: `arn:aws:iam::333861696097:user/ysoz0000-s`
- `STORAGE_AWS_EXTERNAL_ID`: `EFB19770_SFCRole=2_bpw+Ez1tFkjRfMu07ocwd98U2bI=`

But when we checked the Glue catalog integration:
- `GLUE_AWS_IAM_USER_ARN`: `arn:aws:iam::333861696097:user/ysoz0000-s` (same!)
- `GLUE_AWS_EXTERNAL_ID`: `EFB19770_SFCRole=2_7LUmlZdAYceCBhEzS5wOhGy8Dy8=` (different!)

**Eureka moment**: The external volume and catalog integration use the **same IAM user ARN**, but they have **different external IDs**! This is because Snowflake generates separate external IDs for each integration type.

### The Solution: Two Statements, One Trust Policy

The IAM trust policy needed to include **both** external IDs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::333861696097:user/ysoz0000-s"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "EFB19770_SFCRole=2_bpw+Ez1tFkjRfMu07ocwd98U2bI="
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::333861696097:user/ysoz0000-s"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "EFB19770_SFCRole=2_7LUmlZdAYceCBhEzS5wOhGy8Dy8="
        }
      }
    }
  ]
}
```

**Result**: Success! All 24 tables became accessible.

## The Victory: All 24 Tables Working

After applying the final fix, we tested all 24 TPC-DS tables:

```
✅ call_center               - OK (rows: 0)
✅ catalog_page              - OK (rows: 0)
✅ catalog_returns           - OK (rows: 0)
✅ catalog_sales             - OK (rows: 0)
✅ customer                  - OK (rows: 0)
✅ customer_address          - OK (rows: 0)
... (all 24 tables)
```

**All 24 tables accessible from Snowflake!**

## Lessons Learned

### What Worked

1. **Following the working example**: The `simple-glue-setup` provided the blueprint for success
2. **Native GLUE integration**: Using `CATALOG_SOURCE = 'GLUE'` instead of `ICEBERG_REST`
3. **Path matching**: Ensuring external volume `STORAGE_BASE_URL` exactly matches the metadata file location
4. **Comprehensive IAM setup**: Including both external IDs in the trust policy
5. **Read-only external volume**: Setting `ALLOW_WRITES = FALSE` to bypass test file creation

### What Didn't Work

1. **ICEBERG_REST catalog source**: AWS Glue doesn't fully implement the Iceberg REST API
2. **Single external ID in trust policy**: External volume and catalog integration need separate external IDs
3. **Mismatched paths**: The external volume must point to the exact S3 path where metadata files exist
4. **Assuming IAM details were the same**: Even with the same IAM user ARN, different integrations have different external IDs

### Key Insights

1. **Snowflake generates unique external IDs** for each integration type (external volume vs. catalog integration), even when using the same IAM user ARN
2. **The IAM details are hidden in JSON**: The `DESC EXTERNAL VOLUME` output stores IAM details in a JSON field that requires parsing
3. **Native integrations are better**: Snowflake's native GLUE connector is more reliable than REST API-based approaches
4. **Path precision matters**: The external volume path must exactly match the metadata file location structure

## The Complete Solution

### 1. Catalog Integration (Native GLUE)
```sql
CREATE OR REPLACE CATALOG INTEGRATION AWS_GLUE_CATALOG
  CATALOG_SOURCE = 'GLUE'
  CATALOG_NAMESPACE = 'iceberg_performance_test'
  GLUE_CATALOG_ID = '978300727880'
  GLUE_REGION = 'us-east-1'
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::978300727880:role/snowflake-iceberg-role'
  TABLE_FORMAT = 'ICEBERG'
  ENABLED = TRUE;
```

### 2. External Volume (Correct Path)
```sql
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_GLUE_VOLUME
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_storage'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://iceberg-snowflake-perf/iceberg-snowflake-perf/iceberg_glue_format/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::978300727880:role/snowflake-iceberg-role'
    )
  )
  ALLOW_WRITES = FALSE;
```

### 3. IAM Trust Policy (Both External IDs)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<GLUE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<GLUE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

### 4. Table Creation
```sql
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.call_center
EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'call_center'
COMMENT = 'Iceberg AWS Glue-managed call_center table';
```

## Epilogue: The Path Forward

The journey from frustration to success taught us that sometimes the most complex problems have the simplest solutions—once you know where to look. The key was:

1. **Finding a working reference** (simple-glue-setup)
2. **Understanding the differences** (native vs. REST)
3. **Following the data** (parsing DESC output carefully)
4. **Thinking comprehensively** (both external IDs, not just one)

Today, all 24 TPC-DS tables are accessible from Snowflake, ready for performance testing and benchmarking. The setup is documented, the scripts are reusable, and the lessons are learned.

*The end of one journey, and the beginning of the next: performance testing.*

