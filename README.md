# AWS EIA Energy Data Lake & Streaming Pipeline

## Project Overview

This project builds a production-style energy data platform using the **U.S. Energy Information Administration (EIA) API**, **Amazon S3**, **AWS Glue**, and **Amazon Athena**.

The project demonstrates a cloud data-lake workflow for petroleum/energy data:

```text
EIA API
   |
   v
Python Extraction
   |
   v
Amazon S3 - RAW
   |
   v
AWS Glue Crawler
   |
   v
AWS Glue Data Catalog
   |
   v
AWS Glue ETL
   |
   v
Amazon S3 - PROCESSED (Parquet)
   |
   v
Amazon Athena
```

A Kafka/MSK streaming leg was considered as part of P1, but **Amazon MSK was not provisioned because MSK requires a paid AWS account plan**. The AWS batch/data-lake portion of the project was therefore completed without provisioning MSK.

---

# 1. Project Objectives

The main objectives are to:

- Extract petroleum data from the EIA API.
- Store the raw API response in Amazon S3.
- Organize raw data using date-based partition folders.
- Catalog the raw dataset with AWS Glue.
- Flatten the nested EIA API response.
- Transform the dataset into an analytics-friendly structure.
- Store processed data as Parquet with Snappy compression.
- Query and analyze the data using Amazon Athena.
- Demonstrate an AWS cloud data-lake architecture.
- Apply least-privilege access to the project S3 bucket.

---

# 2. Technologies Used

| Technology | Purpose |
|---|---|
| Python | API extraction and S3 ingestion |
| EIA API | Energy/petroleum data source |
| Amazon S3 | Raw and processed data lake storage |
| AWS IAM | Identity and access control |
| AWS CLI | AWS resource verification and authentication |
| AWS Glue Crawler | Schema discovery and Data Catalog |
| AWS Glue Studio | ETL transformation |
| Amazon Athena | SQL analytics |
| Parquet | Processed analytical storage format |
| Snappy | Parquet compression |
| Kafka / Amazon MSK | Planned streaming leg; not provisioned |

---

# 3. AWS Region

The project uses:

```text
eu-north-1
```

AWS Region:

```text
Europe (Stockholm)
```

The S3 bucket and AWS CLI profile were configured for this region.

---

# 4. Project Structure

A simplified project structure is:

```text
Energy-Data-Lake/
│
├── src/
│   └── eia_api_to_s3.py
│
├── .env
├── .gitignore
└── README.md
```

The `.env` file contains configuration/secrets and must not be committed to Git.

---

# 5. Environment Configuration

The project uses `python-dotenv` to load environment variables.

Example `.env`:

```env
EIA_API_KEY=YOUR_EIA_API_KEY
S3_BUCKET_NAME=eia-energy-lake
AWS_DEFAULT_REGION=eu-north-1
```

AWS credentials are not stored in `.env`.

The AWS CLI profile is used by `boto3`:

```bash
export AWS_PROFILE=energy-lake
```

---

# 6. Security Configuration

An IAM user was created for development:

```text
energy-lake-dev
```

A custom policy was created:

```text
EnergyLakeS3Access
```

The policy grants access only to the project bucket.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListEnergyLakeBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::eia-energy-lake"
    },
    {
      "Sid": "ManageEnergyLakeObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::eia-energy-lake/*"
    }
  ]
}
```

This intentionally does not grant:

```text
s3:ListAllMyBuckets
```

because the project only needs access to the specific energy lake bucket.

---

# 7. AWS CLI Configuration

The AWS CLI was configured with the project profile:

```bash
aws configure --profile energy-lake
```

The configured region is:

```text
eu-north-1
```

The active profile can be selected with:

```bash
export AWS_PROFILE=energy-lake
```

Verify the AWS identity:

```bash
aws sts get-caller-identity --profile energy-lake
```

The expected identity is the project IAM user:

```text
arn:aws:iam::<ACCOUNT_ID>:user/energy-lake-dev
```

---

# 8. Verify S3 Access

The project bucket is:

```text
eia-energy-lake
```

Check the bucket directly:

```bash
aws s3 ls s3://eia-energy-lake --profile energy-lake
```

An empty result is valid when the bucket has not yet received data.

---

# 9. Python EIA → S3 Ingestion

The main ingestion script is:

```text
src/eia_api_to_s3.py
```

The script performs two major operations:

1. Extract data from the EIA API.
2. Upload the raw JSON response to Amazon S3.

## Complete ingestion code

```python
import os
import json
import requests
import boto3

from datetime import datetime, timezone
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

BASE_URL = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/"


SERIES_CODES = [
    "MANFPAK1", "MANFPAK2",
    "MCRFP3FM1", "MCRFP3FM2",
    "MCRFP5F1", "MCRFP5F2",
    "MCRFPAK1", "MCRFPAK2",
    "MCRFPAKS1", "MCRFPAKS2",
    "MCRFPAL1", "MCRFPAL2",
    "MCRFPAR1", "MCRFPAR2",
    "MCRFPAZ1", "MCRFPAZ2",
    "MCRFPCA1", "MCRFPCA2",
    "MCRFPCO1", "MCRFPCO2",
    "MCRFPFL1", "MCRFPFL2",
    "MCRFPIL1", "MCRFPIL2",
    "MCRFPIN1", "MCRFPIN2",
    "MCRFPKS1", "MCRFPKS2",
    "MCRFPKY1", "MCRFPKY2",
    "MCRFPLA1", "MCRFPLA2",
    "MCRFPMO1", "MCRFPMO2",
    "MCRFPMT1", "MCRFPMT2",
    "MCRFPND1", "MCRFPND2",
    "MCRFPNE1", "MCRFPNE2",
    "MCRFPNM1", "MCRFPNM2",
    "MCRFPNV1", "MCRFPNV2",
    "MCRFPNY1", "MCRFPNY2",
    "MCRFPOH1", "MCRFPOH2",
    "MCRFPOK1", "MCRFPOK2",
    "MCRFPP11", "MCRFPP12",
    "MCRFPP21", "MCRFPP22",
    "MCRFPP31", "MCRFPP32",
    "MCRFP41", "MCRFP42",
    "MCRFP51", "MCRFP52",
    "MCRFPPA1", "MCRFPPA2",
    "MCRFPSD1", "MCRFPSD2",
    "MCRFPTN1", "MCRFPTN2",
    "MCRFPTX1", "MCRFPTX2",
    "MCRFPUS1", "MCRFPUS2",
    "MCRFPUT1", "MCRFPUT2",
    "MCRFPVA1", "MCRFPVA2",
    "MCRFPWV1", "MCRFPWV2",
    "MCRFPWY1", "MCRFPWY2",
    "MCRFP_SMI_1", "MCRFP_SMI_2",
    "M_EPC0_FPF_SID_MBBL",
    "M_EPC0_FPF_SID_MBBLD",
]


# ============================================================
# Extract
# ============================================================

def fetch_eia_data():

    params = {
        "api_key": EIA_API_KEY,
        "frequency": "annual",
        "data[0]": "value",
        "start": "2010",
        "end": "2026",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
    }

    params_list = list(params.items())

    for series in SERIES_CODES:
        params_list.append(
            ("facets[series][]", series)
        )

    print("Fetching data from EIA API...")

    response = requests.get(
        BASE_URL,
        params=params_list,
        timeout=60
    )

    response.raise_for_status()

    print("EIA API request successful.")

    return response.json()


# ============================================================
# Upload Raw Data to S3
# ============================================================

def upload_to_s3(data):

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    timestamp = datetime.now(timezone.utc)

    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")

    s3_key = (
        f"raw/eia/petroleum/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"eia_petroleum_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    )

    print("Uploading raw data to:")
    print(f"s3://{S3_BUCKET}/{s3_key}")

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print("Upload successful.")

    return s3_key


# ============================================================
# Main
# ============================================================

def main():

    if not EIA_API_KEY:
        raise ValueError("EIA_API_KEY is not configured.")

    if not S3_BUCKET:
        raise ValueError("S3_BUCKET_NAME is not configured.")

    data = fetch_eia_data()

    s3_key = upload_to_s3(data)

    print("\nPipeline completed successfully.")
    print(f"S3 location: s3://{S3_BUCKET}/{s3_key}")


if __name__ == "__main__":
    main()
```

---

# 10. Run the Ingestion Pipeline

Activate the AWS profile:

```bash
export AWS_PROFILE=energy-lake
```

Run:

```bash
python src/eia_api_to_s3.py
```

Successful execution produced:

```text
Fetching data from EIA API...
EIA API request successful.
Uploading raw data to:
s3://eia-energy-lake/raw/eia/petroleum/year=2026/month=10/day=18/eia_petroleum_20261018_001238.json
Upload successful.

Pipeline completed successfully.
```

---

# 11. Raw S3 Data Lake Layer

The raw data is stored using a partition-style structure:

```text
s3://eia-energy-lake/
└── raw/
    └── eia/
        └── petroleum/
            └── year=2026/
                └── month=10/
                    └── day=18/
                        └── eia_petroleum_20261018_001238.json
```

This structure makes the data easier for Glue and Athena to partition and query.

Verify the uploaded object:

```bash
aws s3 ls \
s3://eia-energy-lake/raw/eia/petroleum/year=2026/month=10/day=18/ \
--profile energy-lake
```

The uploaded raw object was approximately:

```text
416,928 bytes
```

---

# 12. Verify S3 Object Metadata

The object can also be inspected using:

```bash
aws s3api head-object \
--bucket eia-energy-lake \
--key raw/eia/petroleum/year=2026/month=10/day=18/eia_petroleum_20261018_001238.json \
--profile energy-lake
```

The object was stored with:

```text
ContentType: application/json
ServerSideEncryption: AES256
```

---

# 13. AWS Glue Crawler

An AWS Glue crawler was created to discover the schema of the raw EIA dataset.

Crawler:

```text
AWS Glue crawler
```

S3 source:

```text
s3://eia-energy-lake/raw/eia/petroleum/
```

Database:

```text
eia_energy_catalog
```

Table:

```text
petroleum
```

Crawler schedule:

```text
On demand
```

The crawler successfully created one table and discovered one partition.

---

# 14. Glue Data Catalog

The Glue Data Catalog database is:

```text
eia_energy_catalog
```

The raw table is:

```text
petroleum
```

The table format is:

```text
JSON
```

The S3 location is:

```text
s3://eia-energy-lake/raw/eia/petroleum/
```

Glue identified the major top-level fields:

```text
response
request
apiversion
exceladdinversion
year
month
day
```

The partition fields are:

```text
year
month
day
```

The important nested structure is:

```text
response.data
```

The actual petroleum records are contained inside this nested array.

---

# 15. Amazon Athena Configuration

Athena was configured to store query results in:

```text
s3://eia-energy-lake/athena-results/
```

This is separate from the raw and processed datasets.

---

# 16. Athena Query 1 — Inspect the Raw Table

The first Athena query was:

```sql
SELECT *
FROM "AwsDataCatalog"."eia_energy_catalog"."petroleum"
LIMIT 10;
```

Purpose:

- Confirm that Athena can access the Glue Data Catalog.
- Verify that the raw table is queryable.
- Inspect the nested EIA API response.

The query completed successfully.

Approximate data scanned:

```text
407.16 KB
```

---

# 17. Athena Query 2 — Flatten the Nested EIA Data

The EIA API response contains the actual records inside:

```text
response.data
```

Athena's `CROSS JOIN UNNEST` was used to expand this nested array.

```sql
SELECT
    d.period,
    d.value,
    d.units,
    d.duoarea,
    d."area-name",
    d.product,
    d."product-name",
    d.process,
    d."process-name",
    d.series,
    d."series-description"
FROM "eia_energy_catalog"."petroleum"
CROSS JOIN UNNEST(response.data) AS t(d)
LIMIT 10;
```

This query converts the nested EIA records into rows that can be analyzed directly.

Example fields include:

```text
period
value
units
duoarea
area-name
product
product-name
process
process-name
series
series-description
```

---

# 18. Athena Query 3 — Aggregate Crude Oil Production

The next query calculated total production by geographic area:

```sql
SELECT
    d."area-name",
    SUM(CAST(d.value AS DOUBLE)) AS total_production,
    d.units
FROM "eia_energy_catalog"."petroleum"
CROSS JOIN UNNEST(response.data) AS t(d)
WHERE d."product-name" = 'Crude Oil'
  AND d.units = 'MBBL'
GROUP BY
    d."area-name",
    d.units
ORDER BY total_production DESC
LIMIT 10;
```

Important:

The EIA dataset contains different geographic aggregation levels. Therefore, the returned areas are **not mutually exclusive** and should not be added together as though they were independent geographic regions.

Example results included:

```text
U.S.
PADD 3
TEXAS
NA
PADD 2
USA-ND
PADD 5
USA-NM
PADD 4
USA-AK
```

---

# 19. Athena CTAS — Create a Processed Parquet Dataset

An Athena CTAS query was also used to create an analytics-friendly processed dataset.

CTAS means:

```text
CREATE TABLE AS SELECT
```

The query was:

```sql
CREATE TABLE eia_energy_catalog.petroleum_processed
WITH (
    format = 'PARQUET',
    external_location = 's3://eia-energy-lake/processed/eia/petroleum/'
)
AS
SELECT
    d.period,
    d.value,
    d.units,
    d.duoarea,
    d."area-name" AS area_name,
    d.product,
    d."product-name" AS product_name,
    d.process,
    d."process-name" AS process_name,
    d.series,
    d."series-description" AS series_description,
    p.year,
    p.month,
    p.day
FROM eia_energy_catalog.petroleum p
CROSS JOIN UNNEST(p.response.data) AS t(d);
```

This converts the nested JSON structure into a flat Parquet dataset.

The processed dataset contains fields such as:

```text
period
value
units
duoarea
area_name
product
product_name
process
process_name
series
series_description
year
month
day
```

---

# 20. Athena Query 4 — Analyze the Processed Dataset

The first aggregation attempt produced a type error because:

```text
value
```

was stored as a string/VARCHAR.

Therefore, it was explicitly converted to `DOUBLE`.

The corrected query was:

```sql
SELECT
    area_name,
    SUM(CAST(value AS DOUBLE)) AS total_production,
    units
FROM eia_energy_catalog.petroleum_processed
WHERE product_name = 'Crude Oil'
  AND process_name = 'Field Production'
  AND units = 'MBBL'
GROUP BY area_name, units
ORDER BY total_production DESC
LIMIT 10;
```

This demonstrates an important analytical data-engineering concept:

```text
Raw API values
      ↓
String/VARCHAR
      ↓
CAST(value AS DOUBLE)
      ↓
Numerical aggregation
```

---

# 21. AWS Glue ETL Job

After the initial Athena CTAS work, an actual AWS Glue ETL pipeline was created.

Glue job:

```text
eia_petroleum_glue_etl
```

Description:

```text
AWS Glue ETL job that flattens and transforms EIA petroleum data and writes the processed dataset to Amazon S3 in Parquet format.
```

The visual workflow is:

```text
AWS Glue Data Catalog
        |
        v
Flatten_EIA_Petroleum_Data
        |
        v
S3_Processed_Petroleum
```

---

# 22. Glue SQL Transformation

The SQL Query transformation was named:

```text
Flatten_EIA_Petroleum_Data
```

Input alias:

```text
myDataSource
```

The transformation flattens `response.data` using `explode`.

```sql
SELECT
    d.period,
    CAST(d.value AS DOUBLE) AS value,
    d.units,
    d.duoarea,
    d.`area-name` AS area_name,
    d.product,
    d.`product-name` AS product_name,
    d.process,
    d.`process-name` AS process_name,
    d.series,
    d.`series-description` AS series_description,
    myDataSource.year,
    myDataSource.month,
    myDataSource.day
FROM myDataSource
LATERAL VIEW explode(response.data) exploded AS d
```

Key transformation:

```text
response.data
       ↓
explode()
       ↓
one EIA record per row
```

The value is also explicitly converted:

```sql
CAST(d.value AS DOUBLE)
```

---

# 23. Glue S3 Target

The processed Glue ETL output is written to:

```text
s3://eia-energy-lake/processed/glue-etl/petroleum/
```

Format:

```text
Parquet
```

Compression:

```text
Snappy
```

The Glue ETL job was configured not to update the Data Catalog automatically because the nested source structure made the automatic catalog update option unsuitable for this transformation.

---

# 24. Glue Job Configuration

Job name:

```text
eia_petroleum_glue_etl
```

Job type:

```text
Spark
```

Glue version:

```text
5.1
```

Language:

```text
Python 3
```

Worker type:

```text
G.1X
```

Requested workers:

```text
10
```

Job bookmarks:

```text
Disabled
```

Retries:

```text
0
```

Generate job insights:

```text
Enabled
```

---

# 25. Glue ETL Execution Result

The Glue job successfully completed.

Result:

```text
SUCCESS
```

Execution duration:

```text
2 minutes 12 seconds
```

The output was written as a Parquet file:

```text
run-179231358578-part-block-0-r-00000-snappy.parquet
```

Output location:

```text
s3://eia-energy-lake/processed/glue-etl/petroleum/
```

The output object was approximately:

```text
11.7 KB
```

---

# 26. Athena Verification of Glue Output

The processed dataset was queried using Athena:

```sql
SELECT *
FROM eia_energy_catalog.petroleum_processed
LIMIT 10;
```

The query completed successfully.

Approximate data scanned:

```text
7.80 KB
```

This demonstrates the advantage of querying the processed Parquet dataset instead of repeatedly scanning the original nested JSON structure.

---

# 27. Athena Query 5 — Count Processed Records

The total number of processed records was verified with:

```sql
SELECT
    COUNT(*) AS total_records
FROM eia_energy_catalog.petroleum_processed;
```

Result:

```text
1344
```

Therefore:

```text
Processed EIA records = 1,344
```

---

# 28. Final Data Lake Structure

The project now contains the following logical layers:

```text
eia-energy-lake/
│
├── raw/
│   └── eia/
│       └── petroleum/
│           └── year=2026/
│               └── month=10/
│                   └── day=18/
│                       └── eia_petroleum_*.json
│
├── processed/
│   ├── eia/
│   │   └── petroleum/
│   │       └── *.parquet
│   │
│   └── glue-etl/
│       └── petroleum/
│           └── *.parquet
│
└── athena-results/
    └── Athena query results
```

---

# 29. Complete End-to-End Workflow

The completed AWS workflow is:

```text
                    ┌──────────────────┐
                    │    EIA API       │
                    │ Petroleum Data   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Python Ingestion │
                    │ requests+boto3   │
                    └────────┬─────────┘
                             │
                             ▼
              ┌────────────────────────────┐
              │ Amazon S3 - RAW            │
              │ JSON                       │
              │ year/month/day partitions  │
              └────────────┬───────────────┘
                           │
                           ▼
                  ┌───────────────────┐
                  │ AWS Glue Crawler  │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │ Glue Data Catalog │
                  │ eia_energy_catalog│
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │ AWS Glue ETL      │
                  │ Flatten + Cast    │
                  └─────────┬─────────┘
                            │
                            ▼
              ┌────────────────────────────┐
              │ S3 - PROCESSED             │
              │ Parquet + Snappy            │
              └────────────┬───────────────┘
                           │
                           ▼
                  ┌───────────────────┐
                  │ Amazon Athena     │
                  │ SQL Analytics     │
                  └───────────────────┘
```

---

# 30. Batch vs Streaming

The original P1 architecture included a streaming leg:

```text
EIA / simulated events
        ↓
Kafka Producer
        ↓
Amazon MSK
        ↓
Kafka Topic
        ↓
Consumer / Processing
```

However, Amazon MSK was **not provisioned** because the AWS account plan required for MSK is paid.

Therefore, the implemented AWS portion of this project is:

```text
EIA API
   ↓
Python
   ↓
S3
   ↓
Glue
   ↓
Parquet
   ↓
Athena
```

The MSK component remains an architectural extension rather than a deployed AWS resource.

---

# 31. Key Data Engineering Concepts Demonstrated

This project demonstrates:

### API ingestion

```text
REST API → Python
```

### Cloud object storage

```text
Python → Amazon S3
```

### Data lake layering

```text
RAW → PROCESSED
```

### Partitioning

```text
year=2026/month=10/day=18
```

### Schema discovery

```text
S3 → Glue Crawler → Data Catalog
```

### Nested-data transformation

```text
response.data → explode / UNNEST → rows
```

### Data type conversion

```text
VARCHAR → DOUBLE
```

### Columnar storage

```text
JSON → Parquet
```

### Compression

```text
Parquet + Snappy
```

### SQL analytics

```text
Athena → SELECT / WHERE / GROUP BY / SUM / COUNT
```

### Cloud security

```text
IAM → least-privilege S3 access
```

---

# 32. Cost and Resource Cleanup

AWS services used in this project are not all permanently free.

Potential charges include:

- Amazon S3 storage and requests
- AWS Glue crawler usage
- AWS Glue ETL/DPU usage
- Amazon Athena data scanned
- Amazon MSK, if provisioned in the future

After completing the project, resources that are no longer needed should be removed or disabled.

The S3 bucket can be retained if the project data is useful for future analysis, while paid compute/streaming resources should not be left running unnecessarily.

---

# 33. Lessons Learned

Important lessons from the project include:

1. **S3 is the foundation of the data lake.**
2. Raw data should be preserved before transformation.
3. Glue Crawler can discover schemas from S3.
4. EIA API responses can contain deeply nested structures that require flattening.
5. Athena can query nested structures using `UNNEST`.
6. Glue ETL can flatten nested records using `explode`.
7. Explicit data-type conversion is important before numerical aggregation.
8. Parquet is more suitable for analytical workloads than raw JSON.
9. Compression reduces processed-data size.
10. Athena's serverless SQL model makes exploratory analytics straightforward.
11. IAM policies should be scoped to the resources actually required.
12. Cloud services should be cleaned up after experimentation to control costs.

---

# 34. Final Project Status

| Component | Status |
|---|---|
| EIA API | ✅ Complete |
| Python extraction | ✅ Complete |
| S3 bucket | ✅ Complete |
| Raw JSON layer | ✅ Complete |
| IAM access | ✅ Complete |
| Glue crawler | ✅ Complete |
| Glue Data Catalog | ✅ Complete |
| Glue ETL | ✅ Complete |
| Processed Parquet | ✅ Complete |
| Athena | ✅ Complete |
| Athena SQL analysis | ✅ Complete |
| Processed record validation | ✅ 1,344 records |
| Amazon MSK | ⏸ Not deployed due to paid account requirement |

---

# 35. Project Outcome

The project successfully implements a cloud-based energy data lake that takes petroleum data from the EIA API, stores the raw response in Amazon S3, catalogs the data with AWS Glue, transforms nested records into an analytical structure, stores the processed dataset as compressed Parquet, and enables SQL analytics through Amazon Athena.

The final architecture demonstrates the core workflow used in modern cloud data engineering:

```text
EXTRACT
   ↓
LOAD
   ↓
CATALOG
   ↓
TRANSFORM
   ↓
STORE
   ↓
QUERY
   ↓
ANALYZE
```

This provides a foundation for building larger energy-data engineering and streaming systems while keeping the raw source data preserved in the lake.
