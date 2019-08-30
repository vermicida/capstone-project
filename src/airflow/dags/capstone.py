from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.capstone_plugin import AggregateTableOperator
from airflow.operators.capstone_plugin import CreateTableOperator
from airflow.operators.capstone_plugin import ImportWeatherOperator
from airflow.operators.dummy_operator import DummyOperator


options = {
    'description': 'Spain\'s weather by month, quarter and year',
    'schedule_interval': '@once',
    'start_date': datetime.now(),
    'catchup': False,
    'default_args': {
        'owner': 'Diego Herrera',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
        'email_on_retry': False,
        'email_on_failure': False
    }
}

with DAG('capstone', **options) as dag:

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    create_weather_table = CreateTableOperator(
        task_id='create_weather_table',
        ddbb_conn_id='ddbb_conn',
        table='weather_staging'
    )

    import_weather = ImportWeatherOperator(
        task_id='import_weather',
        ddbb_conn_id='ddbb_conn',
        aemet_conn_id='aemet_conn',
        from_date='2019-06-20',
        to_date='2019-07-10'
    )

    create_temps_by_month_table = CreateTableOperator(
        task_id='create_temps_by_month_table',
        ddbb_conn_id='ddbb_conn',
        table='temps_by_month'
    )

    create_temps_by_quarter_table = CreateTableOperator(
        task_id='create_temps_by_quarter_table',
        ddbb_conn_id='ddbb_conn',
        table='temps_by_quarter'
    )

    create_temps_by_year_table = CreateTableOperator(
        task_id='create_temps_by_year_table',
        ddbb_conn_id='ddbb_conn',
        table='temps_by_year'
    )

    import_temps_by_month_table = AggregateTableOperator(
        task_id='import_temps_by_month_table',
        ddbb_conn_id='ddbb_conn',
        table='temps_by_month'
    )

    import_temps_by_quarter_table = AggregateTableOperator(
        task_id='import_temps_by_quarter_table',
        ddbb_conn_id='ddbb_conn',
        table='temps_by_quarter'
    )

    import_temps_by_year_table = AggregateTableOperator(
        task_id='import_temps_by_year_table',
        ddbb_conn_id='ddbb_conn',
        table='temps_by_year'
    )

    # Creates the staging table.
    start >> create_weather_table

    # Imports the weather from the AEMET API.
    create_weather_table >> import_weather

    # Creates the fact tables.
    import_weather >> create_temps_by_month_table
    import_weather >> create_temps_by_quarter_table
    import_weather >> create_temps_by_year_table

    # Imports the aggregate weather into the fact tables.
    create_temps_by_month_table >> import_temps_by_month_table
    create_temps_by_quarter_table >> import_temps_by_quarter_table
    create_temps_by_year_table >> import_temps_by_year_table

    # Pipeline end.
    import_temps_by_month_table >> end
    import_temps_by_quarter_table >> end
    import_temps_by_year_table >> end
