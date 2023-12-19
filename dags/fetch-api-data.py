#  Build a function that can take in any country code and fetch the data and also carry out this transformation step.

import requests
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from sqlalchemy import create_engine, types
import os
from dotenv import load_dotenv
import json
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
import pendulum

load_dotenv()

@dag(
        dag_id="data_ingestion",
        schedule= "0 9 * * 0-6",
        start_date=pendulum.datetime(2023, 12, 1, tz="UTC"),
        catchup=False,     
)

def data_ingestion_workflow():

    @task()
    def fetch_data_from_api(country_code):
        baseurl = f'https://date.nager.at/api/v3/publicholidays/2023/{country_code}'
        try:
            response = requests.get(baseurl)
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {e}")

    @task()
    def transform_data(data_from_api):
        if data_from_api:
            # create dataframe with the fetched data
            holiday_df = pd.DataFrame(data_from_api)  

            # day (Monday, Tuesday, Friday etc.) each of those holiday falls in
            holiday_df['date'] = pd.to_datetime(holiday_df['date'])
            holiday_df['day_of_week'] = holiday_df['date'].dt.day_name()
            # df['column_name'] = df['column_name'].astype(str)
            return holiday_df


    @task()
    def load_data(holiday_df): 
        for column in holiday_df.columns:
            if isinstance(holiday_df[column].iloc[0], np.ndarray):
                holiday_df[column] = holiday_df[column].apply(lambda x: x.tolist())

        # write dataframe to db
        conn_id='postgres_conn'
        table_name = 'country_holiday'
        connection = BaseHook.get_connection(conn_id)
        engine = f"postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"

        try:
            existing_tables = pd.read_sql_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", engine)
            if table_name in existing_tables.values:
                 print(f'{table_name} already exists in the database. Skip data loading.')
            else:
                holiday_df.to_sql(table_name, engine,  if_exists = 'replace', index=False)
                print(f"Data successfully stored in {table_name} table in the Database")
            
        except Exception as e:
            print(f"An error occurred: {e}")



    data_from_api = fetch_data_from_api(country_code='NG') 
    transformed_data = transform_data(data_from_api)
    loaded_data = load_data(transformed_data)

    data_from_api  >> transformed_data >> loaded_data
    
dag = data_ingestion_workflow()