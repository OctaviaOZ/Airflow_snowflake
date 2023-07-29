from airflow.utils.dates import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import logging

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
    'rate_to_showflake',
    default_args=default_args,
    description='Rate GBT from EUR once a day',
    schedule_interval=timedelta(days=1),
)


create_table_sql = '''
        CREATE TABLE IF NOT EXISTS exchange_rate (
            RATE NUMERIC(12, 8) NOT NULL, 
            RATE_DATE DATE NOT NULL,
            CREATED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP() NOT NULL,
            UPDATE_AT TIMESTAMP_LTZ(9)
        )
    '''

# Define the task to get the exchange rate
def get_exchange_rate_task():
    try:
        # Call the get_exchange_rate function from get_rate_modules.py
        exchange_rate, exchange_date = get_exchange_rate()

        if exchange_rate is None:
            raise ValueError('Exchange rate is None')

        # Convert the exchange rate to EUR/GBP
        exchange_rate_gbp = 1 / exchange_rate

        # Log the exchange rate and date
        logging.info(f'Exchange rate: {exchange_rate_gbp}, Date: {exchange_date}')
    except Exception as e:
        # Log any errors that occur
        logging.exception('Error getting exchange rate')
        raise ValueError('Error getting exchange rate') from e
        
    return {'exchange_rate': exchange_rate_gbp, 'exchange_date': exchange_date}    


# Create the table in Snowflake if it doesn't exist
def create_table():
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
    return create_table_sql
   

def store_data(**kwargs):
    # Load the DataFrame into Snowflake using the NERGE command

     # Get the exchange rate from the previous task using XCom
    ti = kwargs['task_instance']
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
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connector')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(upsert_sql)
    cursor.close()
    conn.commit()
    conn.close()
    

# we can extand with If-None-Match header to take cached data
def get_exchange_rate():
    """Fetches the latest exchange rate from EUR to the target currency."""
    # we use FREE subscription plan, so we can only use EUR as base currency
    # EUR exchange rate is always 1
    target_currency = "GBP"

    # historical data format YYYY-MM-DD
    # Get the current date
    current_date = datetime.now().date()
    # Format the date as YYYY-MM-DD
    endpoint = current_date.strftime('%Y-%m-%d')

    # we can extand with If-None-Match header to take cached data
    base_url = f"http://api.exchangeratesapi.io/v1/{endpoint}?access_key=f83d838e5f73a39ec92d7365f594afb3" 
    
   
    # Prepare the query parameters for the API request
    # specific target currencies included in the request
    params = {
        "symbols": target_currency
    }

    try:
        # Send the GET request to the API endpoint
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for erroneous responses

        # Parse the JSON response
        data = response.json()

        # Extract the exchange rates
        rates = data.get("rates", {})

        # Get the exchange rate for the target currency
        exchange_rate = rates.get(target_currency)

        exchange_date = data.get("date")

        # return exchange_rate, exchange_date
        return exchange_rate, exchange_date

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None, None
    except Exception as e:
        print(f"Error: {e}")
        return None, None




# Define the tasks for the DAG
with dag:
    # Task to get the exchange rate
    get_exchange_rate_operator = PythonOperator(
        task_id='get_exchange_rate',
        python_callable=get_exchange_rate_task,
        provide_context=True,
    )

    # Task to create the table in Snowflake
    create_table_operator = SnowflakeOperator(
         task_id="create_table",
         sql=create_table_sql,
         snowflake_conn_id="snowflake_connector",
    )

    # Task to store the data in Snowflake
    store_data = PythonOperator(
        task_id='get_guery_to_store_data', 
        python_callable=store_data
    )

    # Set task dependencies
    get_exchange_rate_operator >> create_table_operator >> store_data