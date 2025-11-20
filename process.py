from dotenv import load_dotenv
import os
import boto3
import copernicusmarine as toolbox
from pathlib import Path

# =========================
# PARAMÈTRES VARIABILISÉS
# =========================
load_dotenv()
DATASET_ID = os.environ.get("DATASET_ID")
VARIABLE = os.environ.get("VARIABLE")
START_DATE = os.environ.get("START_DATE")
END_DATE = os.environ.get("END_DATE")
OUTPUT_FOLDER = os.environ.get("OUTPUT_FOLDER", "results")

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")
AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

LOCAL_FILE = f"{OUTPUT_FOLDER}/result_{DATASET_ID}_{VARIABLE}.nc"


def download_data():
    print(f"📌 Dataset: {DATASET_ID}")
    print(f"📌 Variable: {VARIABLE}")

    try:
        ds = toolbox.open_dataset(
            dataset_id=DATASET_ID,
            variables=[VARIABLE],
            start_datetime=START_DATE,
            end_datetime=END_DATE,
        )
    except Exception as e:
        print("❌ Erreur lors de la récupération des données :", e)
        raise

    print("✅ Données récupérées")

    processed = ds[VARIABLE].mean(dim=list(ds[VARIABLE].dims)[-2:])
    processed = processed.to_dataset(name=f"mean_{VARIABLE}")

    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
    processed.to_netcdf(LOCAL_FILE)

    print(f"✅ Fichier local généré : {LOCAL_FILE}")
    return LOCAL_FILE

def upload_to_s3(file_path):
    print("☁️ Upload vers S3...")

#    --- Login to S3 ---
    s3 = boto3.client("s3",endpoint_url =f"{AWS_S3_ENDPOINT}",
                  aws_access_key_id=f"{AWS_ACCESS_KEY_ID}", 
                  aws_secret_access_key=f"{AWS_SECRET_ACCESS_KEY}", 
                  aws_session_token = f"{AWS_SESSION_TOKEN}",
                )

    object_name = f"{OUTPUT_FOLDER}/{os.path.basename(file_path)}"

    s3.upload_file(
        file_path,
        BUCKET_NAME,
        object_name,
    )

    print(f"✅ Upload terminé : s3://{BUCKET_NAME}/{object_name}")


def main():
    local_file = download_data()
    upload_to_s3(local_file)


if __name__ == "__main__":
    main()
