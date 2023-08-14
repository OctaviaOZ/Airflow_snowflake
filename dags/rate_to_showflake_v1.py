from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from typing import Dict, Any
import requests
import logging


class GetExchangeRateOperator(PythonOperator):
    @apply_defaults
    def __init__(self, endpoint: str, target_currency: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.target_currency = target_currency

    def execute(self, context: Dict[str, Any]):
        try:
            # Send the GET request to the API endpoint
            response = requests.get(self.endpoint)
            response.raise_for_status()  # Raise an exception for erroneous responses

            # Parse the JSON response
            data = response.json()

            # Extract the exchange rates
            rates = data.get("rates", {})

            # Get the exchange rate for the target currency
            exchange_rate = rates.get(self.target_currency)

            exchange_date = data.get("date")

            # Log the exchange rate and date
            logging.info(f'Exchange rate: {exchange_rate}, Date: {exchange_date}')

            if exchange_rate is None:
                raise ValueError('Exchange rate is None')

            return {'exchange_rate': exchange_rate, 'exchange_date': exchange_date}

        except requests.exceptions.RequestException as e:
            logging.exception(f"Error fetching data: {e}")
            raise AirflowException(f"Error fetching data: {e}")
        except Exception as e:
            logging.exception(f"Error: {e}")
            raise AirflowException(f"Error: {e}")


class StoreDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, snowflake_conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id

    def execute(self, context: Dict[str, Any]):
        # Load the DataFrame into Snowflake using the NERGE command

        # Get the exchange rate from the previous task using XCom
        ti = context['ti']
        rate_values = ti.xcom_pull(task_ids='get_exchange_rate')
        exchange_rate = rate_values.get('exchange_rate')
        exchange_date = rate_values.get('exchange_date')

        upsert_sql = f'''
            MERGE INTO exchange_rate AS tgt
            USING (SELECT '{exchange_date}' AS scr_rate_date)
            ON tgt.RATE_DATE = scr_rate_date
            WHEN MATCHED THEN
                UPDATE SET RATE = {exchange_rate}, UPDATE_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (RATE, RATE_DATE) VALUES ({exchange_rate}, '{exchange_date}');'''

        # Push the data to Snowflake using the SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(upsert_sql)
        cursor.close()
        conn.commit()
        conn.close()


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dag_rate_to_showflake_v1',
    default_args=default_args,
    description='Rate GBT from EUR once a day',
    schedule_interval='@daily',
)

# Define the tasks for the DAG
with dag:
    # Task to get the exchange rate
    get_exchange_rate_operator = GetExchangeRateOperator(
        task_id='get_exchange_rate',
        endpoint='http://api.exchangeratesapi.io/v1/{{ ds }}?access_key=f83d838e5f73a39ec92d7365f594afb3',
        target_currency='GBP',
        provide_context=True,
    )

    # Create the table in Snowflake if it doesn't exist
    # rate_date with no time elements
    # created_at and update_at without a timezone,  UTC time
    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS exchange_rate (
            RATE NUMERIC(12, 8) NOT NULL, 
            RATE_DATE DATE NOT NULL,
            CREATED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP() NOT NULL,
            UPDATE_AT TIMESTAMP_LTZ(9)
        )
    '''

    # Task to create the table in Snowflake
    create_table_operator = SnowflakeOperator(
         task_id="create_table",
         sql=create_table_sql,
         snowflake_conn_id="snowflake_conn",
    )

    # Task to store the data in Snowflake
    store_data_operator = StoreDataOperator(
        task_id='store_data',
        snowflake_conn_id='snowflake_conn',
    )

    # Set task dependencies
    get_exchange_rate_operator >> create_table_operator >> store_data_operator