import requests
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import json
from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
import pendulum

@dag(
        dag_id="data_ingestion",
        schedule= "0 9 * * 0-6",
        start_date=pendulum.datetime(2023, 12, 1, tz="UTC"),
        catchup=False,     
)

def data_ingestion_workflow():

    @task()
    def fetch_data_from_api(country_code):
        url = f'https://date.nager.at/api/v3/publicholidays/2024/{country_code}'
        try:
            response = requests.get(url)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {e}")

    @task()
    def transform_data(data_from_api):
        if data_from_api:
            holiday_df = pd.DataFrame(data_from_api)  

            # day (Monday, Friday etc.) each of those holiday falls in
            holiday_df['date'] = pd.to_datetime(holiday_df['date'], errors='coerce')
            holiday_df['dayofweek'] = holiday_df['date'].dt.day_name()
            holiday_df['types'] = holiday_df['types'].astype(str)
            holiday_df['types'] = holiday_df['types'].replace(r'\W', '', regex=True)
            return holiday_df

    @task()
    def load_data(holiday_df): 
        connection = None
        cursor = None
        try:
            # connect to postgresdb on airflow
            table_name = 'public_holiday'
            conn_id = 'postgres_conn'
            engine = BaseHook.get_connection(conn_id)
            conn_string = f"dbname='{engine.schema}' user='{engine.login}' host='{engine.host}' password='{engine.password}' port='{engine.port}'"
            connection = psycopg2.connect(conn_string)
            cursor = connection.cursor()

            # create table
            create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join([f'{column_name} VARCHAR' for column_name in holiday_df.columns])});"
            cursor.execute(create_table)
            connection.commit()
            print('Table created successfully or already exists')

            # insert column and values
            column = ', '.join(holiday_df.columns)
            placeholders = ', '.join(['%s'] * len(holiday_df.columns))
            query = f"INSERT INTO {table_name} ({column}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

            for row in holiday_df.values.tolist():
                cursor.execute(query, row)
            connection.commit()
            print('Data inserted successfully')

        except (Exception, psycopg2.Error) as error:
            print('Error while connecting to PostgreSQL', error)

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection closed")

    data_from_api = fetch_data_from_api(country_code='NG') 
    transformed_data = transform_data(data_from_api)
    loaded_data = load_data(transformed_data)

    data_from_api >> transformed_data >> loaded_data
    
dag = data_ingestion_workflow()