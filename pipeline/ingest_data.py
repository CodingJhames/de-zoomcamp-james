
import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# df = pd.read_csv(
#     url,
#     dtype=dtype,
#     parse_dates=parse_dates
# )


#df['tpep_pickup_datetime']

#df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}


parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pguser', default='root', help='PostgreSQL user')
@click.option('--pgpassword', default='root', help='PostgreSQL password')
@click.option('--pghost', default='localhost', help='PostgreSQL host')
@click.option('--pgport', default=5432, type=int, help='PostgreSQL port')
@click.option('--pgdb', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data (1-12)')
@click.option('--chunksize', default=100000, type=int, help='Size of data chunks')
def run(pguser, pgpassword, pghost, pgport, pgdb, target_table, year, month, chunksize):
    pg_user = pguser
    pg_pass = pgpassword
    pg_host = pghost
    pg_port = pgport
    pg_db = pgdb

    year = year
    month = month

    target_table = target_table

    chunksize = chunksize


    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')


if __name__ == '__main__':
    run()




