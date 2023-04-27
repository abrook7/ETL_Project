import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG 
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


'''To launch: docker-compose up -d'''

default_args = {
    'owner': 'abrook1',
    'retries': 0,
    'retry delay': timedelta(minutes=5)
}

'''
Function to call the coin api and return the ohlcv response json data,
Transform response data into a dataframe and modifying some column dtypes,
and Load df into airflow postgres database
'''
def api_to_postgres(coin_id, coin, **kwargs):
    #api parameters
    period = '5MIN'
    coin_id = coin_id
    api_key = Variable.get("api_key")

    #call api
    url = f'https://rest.coinapi.io/v1/ohlcv/{coin_id}/latest?period_id={period}&limit=1'
    headers = {'X-CoinAPI-Key' : api_key}

    response = requests.get(url, headers=headers)
    print(response.headers)
    response_json = response.json()

    #transform api response to df
    ohlcv_df = pd.DataFrame(response_json)

    ohlcv_df['time_period_start'] = pd.to_datetime(ohlcv_df['time_period_start']).dt.tz_convert(None)
    ohlcv_df['time_period_end'] = pd.to_datetime(ohlcv_df['time_period_end']).dt.tz_convert(None)
    ohlcv_df['time_open'] = pd.to_datetime(ohlcv_df['time_open']).dt.tz_convert(None)
    ohlcv_df['time_close'] = pd.to_datetime(ohlcv_df['time_close']).dt.tz_convert(None)
    ohlcv_df['period_date'] = ohlcv_df['time_period_start'].dt.date

    #load df to local postgres database
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    engine = hook.get_sqlalchemy_engine()
    ohlcv_df.to_sql(name=f"{coin}_prices",con=engine, if_exists='append', index=False)
    return

def postgres_to_s3(coin, ds, ts_nodash,**kwargs):
    #save postgres data to csv file
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    engine = hook.get_sqlalchemy_engine()
    sql = f'''
        select * 
        from {coin}_prices
        where period_date = '{ds}'
    '''
    df = pd.read_sql_query(sql=sql, con=engine, index_col='id')
    csv_name = f'{coin}_price_data_{ts_nodash}.csv'
    filename = f'./{csv_name}'
    df.to_csv(filename, header=False)

    #load csv file to s3
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        filename=filename,
        key=f'{coin}_price_data_{ts_nodash}.csv',
        bucket_name="crypto-price-bucket-abro",
        replace=True
        )
    return csv_name


with DAG(
    default_args=default_args,
    dag_id='crypto_prices_dag',
    description='dag orchestrating crypto price ETL',
    start_date=datetime(2023, 4, 25),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    create_btc_postgres_table = PostgresOperator(
        task_id='create_btc_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql='''
            create table if not exists bitcoin_prices (
                id SERIAL NOT NULL,
                time_period_start timestamp,
                time_period_end timestamp,
                time_open timestamp,
                time_close timestamp,
                price_open int,
                price_high int,
                price_low int,
                price_close int,
                volume_traded float,
                trades_count int,
                period_date date,
                PRIMARY KEY(id)
            );
        '''
    )

    create_eth_postgres_table = PostgresOperator(
        task_id='create_eth_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql='''
            create table if not exists ethereum_prices (
                id SERIAL NOT NULL,
                time_period_start timestamp,
                time_period_end timestamp,
                time_open timestamp,
                time_close timestamp,
                price_open int,
                price_high int,
                price_low int,
                price_close int,
                volume_traded float,
                trades_count int,
                period_date date,
                PRIMARY KEY(id)
            );
        '''
    )

    create_xrp_postgres_table = PostgresOperator(
        task_id='create_xrp_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql='''
            create table if not exists ripple_prices (
                id SERIAL NOT NULL,
                time_period_start timestamp,
                time_period_end timestamp,
                time_open timestamp,
                time_close timestamp,
                price_open int,
                price_high int,
                price_low int,
                price_close int,
                volume_traded float,
                trades_count int,
                period_date date,
                PRIMARY KEY(id)
            );
        '''
    )

    btc_load = PythonOperator(
        task_id='api_btc_to_postgres',
        python_callable=api_to_postgres,
        op_kwargs={'coin_id':'BITSTAMP_SPOT_BTC_USD','coin':'bitcoin'}
    )

    eth_load = PythonOperator(
        task_id='api_eth_to_postgres',
        python_callable=api_to_postgres,
        op_kwargs={'coin_id':'BITSTAMP_SPOT_ETH_USD', 'coin':'ethereum'}
    )

    xrp_load = PythonOperator(
        task_id='api_xrp_to_postgres',
        python_callable=api_to_postgres,
        op_kwargs={'coin_id':'BITSTAMP_SPOT_XRP_USD', 'coin':'ripple'}
    )

    create_btc_postgres_table >> btc_load
    create_eth_postgres_table >> eth_load
    create_xrp_postgres_table >> xrp_load

with DAG(
    default_args=default_args,
    dag_id='crypto_prices_load_to_s3_redshift',
    description='dag orchestrating postgres table info to s3 and redshift',
    start_date=datetime(2023, 4, 26),
    schedule_interval= "@daily",
    catchup=False
) as dag:
    btc_to_s3 = PythonOperator(
        task_id='btc_postgres_to_s3',
        python_callable=postgres_to_s3,
        do_xcom_push= True,
        op_kwargs={'coin':'Bitcoin'}
    )

    eth_to_s3 = PythonOperator(
        task_id='eth_postgres_to_s3',
        python_callable=postgres_to_s3,
        do_xcom_push= True,
        op_kwargs={'coin':'Ethereum'}
    )

    xrp_to_s3 = PythonOperator(
        task_id='xrp_postgres_to_s3',
        python_callable=postgres_to_s3,
        do_xcom_push= True,
        op_kwargs={'coin':'Ripple'}
    )

    btc_redshift_table = RedshiftDataOperator(
        task_id="create_table_bitcoin_prices",
        cluster_identifier="crypto-redshift-cluster",
        database="mydb",
        db_user="project_test1",
        aws_conn_id="s3_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS bitcoin_prices(
                id int NOT NULL,
                time_period_start timestamp,
                time_period_end timestamp,
                time_open timestamp,
                time_close timestamp,
                price_open int,
                price_high int,
                price_low int,
                price_close int,
                volume_traded float,
                trades_count int,
                period_date date
            )
        """
    )

    eth_redshift_table = RedshiftDataOperator(
        task_id="create_table_ethereum_prices",
        cluster_identifier="crypto-redshift-cluster",
        database="mydb",
        db_user="project_test1",
        aws_conn_id="s3_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS ethereum_prices(
                id int NOT NULL,
                time_period_start timestamp,
                time_period_end timestamp,
                time_open timestamp,
                time_close timestamp,
                price_open int,
                price_high int,
                price_low int,
                price_close int,
                volume_traded float,
                trades_count int,
                period_date date
            )
        """
    )

    xrp_redshift_table = RedshiftDataOperator(
        task_id="create_table_xrp_prices",
        cluster_identifier="crypto-redshift-cluster",
        database="mydb",
        db_user="project_test1",
        aws_conn_id="s3_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS ripple_prices(
                id int NOT NULL,
                time_period_start timestamp,
                time_period_end timestamp,
                time_open timestamp,
                time_close timestamp,
                price_open int,
                price_high int,
                price_low int,
                price_close int,
                volume_traded float,
                trades_count int,
                period_date date
            )
        """
    )

    btc_to_redshift= S3ToRedshiftOperator(
        task_id="transfer_btc_to_redshift",
        redshift_conn_id="redshift_conn",
        aws_conn_id="s3_conn",
        s3_bucket="crypto-price-bucket-abro",
        s3_key="{{ti.xcom_pull(task_ids='btc_postgres_to_s3')}}",
        schema="public",
        table="bitcoin_prices",
        copy_options=["csv"]
    )

    eth_to_redshift= S3ToRedshiftOperator(
        task_id="transfer_eth_to_redshift",
        redshift_conn_id="redshift_conn",
        aws_conn_id="s3_conn",
        s3_bucket="crypto-price-bucket-abro",
        s3_key="{{ti.xcom_pull(task_ids='eth_postgres_to_s3')}}",
        schema="public",
        table="ethereum_prices",
        copy_options=["csv"]
    )

    xrp_to_redshift= S3ToRedshiftOperator(
        task_id="transfer_xrp_to_redshift",
        redshift_conn_id="redshift_conn",
        aws_conn_id="s3_conn",
        s3_bucket="crypto-price-bucket-abro",
        s3_key="{{ti.xcom_pull(task_ids='xrp_postgres_to_s3')}}",
        schema="public",
        table="ripple_prices",
        copy_options=["csv"]
    )

    btc_to_s3 >> btc_redshift_table >> btc_to_redshift
    eth_to_s3 >> eth_redshift_table >> eth_to_redshift
    xrp_to_s3 >> xrp_redshift_table >> xrp_to_redshift