import argparse
import pandas as pd
import os
from sqlalchemy import create_engine


def main(params):
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db_name = params.db_name
	tbl_name = params.tbl_name
	url = params.url
	csv_name = "output.parquet"

	os.system(f"wget {url} -O {csv_name}")

	nyc_trip = pd.read_parquet(csv_name)
	# nyc_trip = nyc_trip.head(100)
	nyc_trip.head()
	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
	engine.connect()
	print(pd.io.sql.get_schema(nyc_trip, tbl_name, con=engine))
	nyc_trip.to_sql(name=tbl_name, con=engine, if_exists='replace')



if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Ingest CSV data to postgres")
	parser.add_argument('--user', help='user name for postgres')
	parser.add_argument('--password', help='password for postgres')
	parser.add_argument('--host', help='host for postgres')
	parser.add_argument('--port', help='port for postgres')
	parser.add_argument('--db_name', help='database name')
	parser.add_argument('--tbl_name', help='table name')
	parser.add_argument('--url', help='url for csv')
	args = parser.parse_args()
	main(args)