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
    "MCRFPMS1", "MCRFPMS2",
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
    "MCRFPP41", "MCRFPP42",
    "MCRFPP51", "MCRFPP52",
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