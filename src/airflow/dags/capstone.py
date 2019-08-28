from airflow import DAG
from datetime import datetime
from airflow.operators.capstone_plugin import (
    CreateTableOperator,
    ImportWeatherOperator
)
from airflow.operators.dummy_operator import DummyOperator


options = {
    'schedule_interval': '@once',
    'start_date': datetime(2019, 7, 1),
    'catchup': False
}

with DAG('capstone', **options) as dag:

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    create_weather_table = CreateTableOperator(
        task_id='create_weather_table',
        ddbb_conn_id='ddbb-staging-conn',
        table='weather'
    )

    import_weather = ImportWeatherOperator(
        task_id='import_weather',
        ddbb_conn_id='ddbb-staging-conn',
        aemet_conn_id='aemet-conn',
        from_date='2019-07-01',
        to_date='2019-07-31'
    )

    create_daily_temps_by_city_table = CreateTableOperator(
        task_id='create_daily_temps_by_city_table',
        ddbb_conn_id='ddbb-conn',
        table='daily_temps_by_city'
    )

    create_weekly_temps_by_city_table = CreateTableOperator(
        task_id='create_weekly_temps_by_city_table',
        ddbb_conn_id='ddbb-conn',
        table='weekly_temps_by_city'
    )

    create_monthly_temps_by_city_table = CreateTableOperator(
        task_id='create_monthly_temps_by_city_table',
        ddbb_conn_id='ddbb-conn',
        table='monthly_temps_by_city'
    )

    create_quarterly_temps_by_city_table = CreateTableOperator(
        task_id='create_quarterly_temps_by_city_table',
        ddbb_conn_id='ddbb-conn',
        table='quarterly_temps_by_city'
    )

    create_yearly_temps_by_city_table = CreateTableOperator(
        task_id='create_yearly_temps_by_city_table',
        ddbb_conn_id='ddbb-conn',
        table='yearly_temps_by_city'
    )

    start >> create_weather_table
    create_weather_table >> import_weather
    import_weather >> create_daily_temps_by_city_table
    import_weather >> create_weekly_temps_by_city_table
    import_weather >> create_monthly_temps_by_city_table
    import_weather >> create_quarterly_temps_by_city_table
    import_weather >> create_yearly_temps_by_city_table
    create_daily_temps_by_city_table >> end
    create_weekly_temps_by_city_table >> end
    create_monthly_temps_by_city_table >> end
    create_quarterly_temps_by_city_table >> end
    create_yearly_temps_by_city_table >> end
