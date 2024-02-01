import configparser
import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
import meetup.api

from src.trend_analyzer import TrendAnalyzer


# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')


def create_spark_session():
    """Create and return a SparkSession with the desired configuration."""
    try:
        return (SparkSession.builder
                .config("spark.executor.memory", 
                        config.get('Spark', 'executor_memory'))
                .config("spark.driver.memory", 
                        config.get('Spark', 'driver_memory'))
                .appName("TrendingTopicsAnalyzer")
                .getOrCreate())
    except Exception as e:
        logging.error(f"Error creating SparkSession: {e}")
        return None


class Application:
    def __init__(self, spark_session: SparkSession):
        """Initialize the Application with a SparkSession and a JSON file path."""
        self.spark_session = spark_session
        self.trend_analyzer = TrendAnalyzer(spark_session)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.spark_session.sparkContext.setLogLevel("ERROR")
        self.spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

    def fetch_api_data(self):
        # Fetch data from Meetup API
       
        # Load environment variables from .env file
        load_dotenv()

        # Get API key and secret from environment variables
        # url = 'https://stream.meetup.com/2/rsvps'
        api_key = os.getenv('MEETUP_API_KEY')
        api_secret = os.getenv('MEETUP_API_SECRET')

        # Make sure the variables are set
        if api_key is None:
            self.logger.error(f"API key or secret not found in environment variables.")
            return None

        params = {
            'key': api_key,  # Meetup API key or OAuth token
            'secret': api_secret
        }

        try:
            meetup_client = meetup.api.Client(api_key)
            # Get events
            rsvps = meetup_client.GetRsvps({'rsvp':"yes"})
            return [rsvp.__dict__ for rsvp in rsvps.results]

        #response = requests.get(url, params=params)
        #if response.status_code == 200:
        #return response.json()
        #else:
        #    self.logger.error(f"Error fetching API data: {response.status_code}")
        #    return None        
        except Exception as e:
            self.logger.error(f"Error getting rsvps: {e}")
            return None

    def get_data(self, data_source: str):
        try:
            if data_source == 'API':
                data = self.fetch_api_data()
                if data is None:
                    raise ValueError("No data fetched from API")
            else:  
                data = data_source
        
            # Use safe_json_parse for robust JSON parsing
            # we assume the streaming data is in JSON format
            if data_source == 'API':
                raw_data = self.spark_session.readStream.option("mode", "DROPMALFORMED")\
                    .json(data)
            else:
                raw_data = self.spark_session.read.option("mode", "DROPMALFORMED")\
                    .json(data)

            # Check if the DataFrame is empty
            if raw_data.rdd.isEmpty():
                self.logger.info("No data found")
                return None
            else:
                return raw_data
        except Exception as e:
            self.logger.error(f"Error getting data: {e}")
            return None   


def main():
    spark = create_spark_session()
    if spark is None:
        logging.error("Unable to create SparkSession. Exiting.")
        return
    
    # Choose source: API or local file
    # for futher can be Used a command-line argument for the data source
    data_source = config.get('Data', 'source')

    app = Application(spark)
    raw_data = app.get_data(data_source)

    processed_data = app.trend_analyzer.process_data(raw_data)
    del raw_data

    # Define the window duration and sliding interval
    window_duration = config.get('Analysis', 'window_duration')
    slide_duration = config.get('Analysis', 'slide_duration')
    n = config.getint('Analysis', 'n')
    filter_name = config.get('Filter', 'name')
    filter_value = config.get('Filter', 'value')

    app.trend_analyzer.analyze_and_plot(processed_data, window_duration, slide_duration, n)

    filtered_data = app.trend_analyzer.filter_by_location(processed_data, filter_name, filter_value)
    app.trend_analyzer.analyze_and_plot(filtered_data, window_duration\
                                        , slide_duration, n, filter_name, filter_value)

    # Stop the SparkSession       
    spark.stop()


if __name__ == '__main__':
    main()
