from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from capstone_plugin.helpers import queries
from capstone_plugin.hooks.aemet import AemetHook


class ImportWeatherOperator(BaseOperator):

    """
    This operator handles the import process of the weather data, retrieved
    from the AEMET API, to the application database.
    """

    @apply_defaults
    def __init__(
        self,
        ddbb_conn_id,
        aemet_conn_id,
        from_date,
        to_date,
        *args,
        **kwargs
    ):

        """
        Initializes a new instance of the class ImportWeatherOperator.

        Parameters:
            ddbb_conn_id (str): The PostgreSQL database connection identifier.
            aemet_conn_id (str): The AEMET API connection identifier.
            from_date (str): The initial date, given in ISO 8601 format.
            to_date (str): The ending date, given in ISO 8601 format.
        """

        super().__init__(*args, **kwargs)
        self._ddbb_conn_id = ddbb_conn_id
        self._aemet_conn_id = aemet_conn_id
        self._from_date = from_date
        self._to_date = to_date

    def execute(self, context):

        """
        Retrieves the weather data and loads it into the database.

        Parameters:
            context (dict): Contains info related to the task instance.
        """

        weather_df = AemetHook(self._aemet_conn_id).run(
            self._from_date,
            self._to_date
        )
        query = queries.INSERTION_QUERIES['weather_staging']
        postgres = PostgresHook(self._ddbb_conn_id)

        for index, row in weather_df.iterrows():
            postgres.run(
                query,
                parameters=row,
                autocommit=True
            )
