from sqlalchemy import create_engine
import pandas as pd
import pyarrow.parquet as pq
from time import time
import argparse
import os

def main(params):  
    user = params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table=params.table
    url=params.url

    #download file 
    file_name = "nyc_taxi.parquet" 

    os.system(f"wget {url} -O {file_name}")

    # engine =  create_engine("postgresql://root:root@localhost:5432/ny_taxi")
    engine =  create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    #upload in batches
    # parquet_file = pq.ParquetFile(f"D:\DataEnggineering\yellow_tripdata_2023-01.parquet")
    parquet_file = pq.ParquetFile(file_name)
    batch_no = 0
    for batch in parquet_file.iter_batches():
        t_start = time()
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table, con=engine, if_exists='append')
        batch_no+=1
        t_end = time()
        print(f"shape:{batch_df.shape}; batch no#{batch_no}:time {t_end-t_start:3f} second")
    print("All done!") 

if __name__=="__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table', help='name of the destination table')
    parser.add_argument('--url', help='url of parquet file')

    args = parser.parse_args()

    main(args)

