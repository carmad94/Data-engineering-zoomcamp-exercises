from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(
        dataset_url,
        parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"],
        infer_datetime_format=True
    )
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


# @task(log_prints=True)
# def clean(df: pd.DataFrame) -> pd.DataFrame:
#     df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
#     df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
#     print(df.head(2))
#     return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task
def write_gcs(path: Path)->None:
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color:str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    # df_clean = clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)
