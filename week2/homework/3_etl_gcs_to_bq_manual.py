from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"{gcs_path}")


@task()
def read_gcs(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("level-agent-375808")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="level-agent-375808",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_web_to_gcs(year: int, month: int, color:str) -> int:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    path = extract_from_gcs(color, year, month)
    df = read_gcs(path)
    write_bq(df)
    print(len(df))
    return len(df)


@flow()
def etl_gcs_to_bq(
    months: list[int] = [1, 2],
    year: int = 2021,
    color: str = "yellow"
):
    rows_uploaded = 0
    for month in months:
        rows_uploaded+=etl_web_to_gcs(year, month, color)
    print(f"Number of rows processed {rows_uploaded}")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_gcs_to_bq(months, year, color)
