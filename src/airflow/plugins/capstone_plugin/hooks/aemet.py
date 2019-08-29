import pandas as pd
from airflow.hooks.http_hook import HttpHook


class AemetHook(HttpHook):

    """
    This hook handles the communication with the OpenData API provided
    by AEMET, the Meteorology Statal Agency of Spain. You can get the weather
    data gathered by thousands of stations spread all over the country.

    Once the weather data is retrieved, it's loaded into a DataFrame to
    ease the wrangling.
    """

    def __init__(self, aemet_conn_id):

        """
        Initializes a new instance of the class AemetHook.

        Parameters:
            aemet_conn_id (str): The AEMET API connection identifier.
        """

        conn_id = self.get_connection(aemet_conn_id)
        self._aemet_api_key = conn_id.extra_dejson.get('api_key')
        super().__init__(method='GET', http_conn_id=aemet_conn_id)

    def get_conn(self, headers):

        """
        Gets a connection to the AEMET API using the appropriate API key.

        Parameters:
            headers (dict): The headers to use in the requests.

        Returns:
            (Session): A ready-to-use connection.
        """

        if self._aemet_api_key:
            headers = {'api_key': self._aemet_api_key}
        return super().get_conn(headers)

    def run(self, from_date, to_date):

        """
        Retrieve all the weather data gathered between the given dates.

        Parameters:
            from_date (str): The initial date, given in ISO 8601 format.
            to_date (str): The ending date, given in ISO 8601 format.

        Returns:
            (DataFrame): A dataframe with the retrieved weather info.
        """

        uri = self._get_weather_endpoint(from_date, to_date)
        weather = self._get_weather_data(uri)
        return self._weather_to_dataframe(weather)

    def _get_weather_endpoint(self, from_date, to_date):

        """
        Gets the URI where the requested weather data is available.

        Parameters:
            from_date (str): The initial date, given in ISO 8601 format.
            to_date (str): The ending date, given in ISO 8601 format.

        Returns:
            (str): The endpoint where the weather data can be retrieved.
        """

        endpoint = '/'.join((
            'opendata',
            'api',
            'valores',
            'climatologicos',
            'diarios',
            'datos',
            'fechaini',
            '{}T00:00:00UTC'.format(from_date),
            'fechafin',
            '{}T23:59:59UTC'.format(to_date),
            'todasestaciones'
        ))
        response = super().run(endpoint).json()

        if 'datos' not in response:
            raise BaseException('Error while requesting the AEMET API: {}'.format(response))  # noqa: E501

        return response['datos']

    def _get_weather_data(self, resource):

        """
        Retrieves the weather data from the given resource endpoint.

        Parameters:
            resource (src): The enpoint where the weather data is available.

        Returns:
            (dict): The wheather data.
        """

        endpoint = 'opendata{}'.format(resource.split('opendata')[-1])
        return super().run(endpoint).json()

    @classmethod
    def _str_is_numeric(cls, text):

        """
        Checks if the given text is a number.

        Parameters:
            text (str): A text.

        Returns:
            (bool): True if the text can be converted to a
                float type. Otherwise, False.
        """

        return isinstance(text, str) and text.replace(',', '', 1).isdigit()

    def _normalize_values(self, row):

        """
        Normalizes the values contained in the given row.

        Parameters:
            row (dict): A weather dataframe row.

        Returns:
            (dict): The same row, but properly normalized.
        """

        # Splits the date into separate columns.
        row['anio'], row['mes'], row['dia'] = row['fecha'].split('-')

        # Fixes the numeric columns format.
        numeric_columns = (
            'altitud',
            'tmed',
            'prec',
            'tmin',
            'tmax',
            'dir',
            'velmedia',
            'racha',
            'presMax',
            'presMin'
        )
        for column in numeric_columns:
            if not isinstance(row[column], float):
                row[column] = row[column].replace(',', '.') \
                    if self._str_is_numeric(row[column]) \
                    else None

        return row

    def _weather_to_dataframe(self, weather):

        """
        Loads the given weather dictionary into a Pandas DataFrame.

        Parameters:
            weather (dict): A dictionary with weather data from AEMET.

        Return:
            (DataFrame): A dataframe with the relevant weather data.
        """

        # Loads the weather dictionary into a DataFrame.
        df = pd.DataFrame.from_dict(weather)

        # Normalizes the values.
        df = df.apply(self._normalize_values, axis=1)

        # Gets only the relevant columns.
        relevant_columns = [
            'fecha',
            'anio',
            'mes',
            'dia',
            'nombre',
            'provincia',
            'altitud',
            'tmed',
            'prec',
            'tmin',
            'tmax',
            'dir',
            'velmedia',
            'racha',
            'presMax',
            'presMin'
        ]
        df = df[relevant_columns]

        # Casts the columns to the right type.
        df = df.astype({
            'fecha': 'object',
            'anio': 'int64',
            'mes': 'int64',
            'dia': 'int64',
            'nombre': 'object',
            'provincia': 'object',
            'altitud': 'float64',
            'tmed': 'float64',
            'prec': 'float64',
            'tmin': 'float64',
            'tmax': 'float64',
            'dir': 'float64',
            'velmedia': 'float64',
            'racha': 'float64',
            'presMax': 'float64',
            'presMin': 'float64'
        })

        return df
