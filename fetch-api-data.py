#  Build a function that can take in any country code and fetch the data and also carry out this transformation step.

import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from  airflow.operators.python import PythonOperator 
import pendulum

load_dotenv()

@dag(
        dag_id="data_ingestion",
        schedule= "0 9 * * 0-6",
        start_date=pendulum.datetime(2023, 11, 1, tz="UTC"),
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
            return holiday_df

    @task()
    def load_data(holiday_df): 
        # connect to postgresql dbms
        db_params = {
        'dbname': os.getenv('dbname'),
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'host': os.getenv('host'), 
        'port': os.getenv('port'),     
        }

        conn = psycopg2.connect(**db_params)
        conn.autocommit = True
        cursor  = conn.cursor()

        try:
            new_db_name = 'public_holiday'
            cursor.execute(f"select 1 from pg_catalog.pg_database where datname='{new_db_name}'")
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(f'create database {new_db_name}')
                
                # connect to new db
                db_params['db_name']= new_db_name
                engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['db_name']}")

                # write dataframe to db
                table_name = 'country_holiday'
                holiday_df.to_sql(table_name, engine,  if_exists = 'replace')
                print(f"Data successfully stored in {table_name} table in the {new_db_name} Database")
        
            else:
                print(f'Database {new_db_name} already exists')
        except psycopg2.Error as e:
            print(f'An error occured: {e}')
        finally:
            conn.close()


    data_from_api = fetch_data_from_api(country_code='NG') 
    transformed_data = transform_data(data_from_api)
    loaded_data = load_data(transformed_data)

    data_from_api  >> transformed_data >> loaded_data
    
dag = data_ingestion_workflow()