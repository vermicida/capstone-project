from airflow import DAG
from datetime import datetime
from airflow.operators.capstone_plugin import (
    CreateTableOperator,
    ImportWeatherOperator
)
from airflow.operators.dummy_operator import DummyOperator


options = {
    'schedule_interval': '@monthly',
    'start_date': datetime(2019, 7, 1),
    'catchup': False
}

with DAG('capstone', **options) as dag:

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    create_weather_staging_table = CreateTableOperator(
        task_id='create_weather_staging_table',
        ddbb_conn_id='ddbb-conn',
        table='weather_staging'
    )

    import_weather = ImportWeatherOperator(
        task_id='import_weather',
        ddbb_conn_id='ddbb-conn',
        aemet_conn_id='aemet-conn',
        from_date='2019-07-31',
        to_date='2019-07-31'
    )

    start >> create_weather_staging_table
    create_weather_staging_table >> import_weather
    import_weather >> end
