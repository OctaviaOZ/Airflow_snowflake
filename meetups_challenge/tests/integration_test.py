from pyspark.sql import SparkSession
from src.trend_analyzer import TrendAnalyzer


def integration_test():
    """
    Performs an integration test of the trending_topics project.
    """
    # Initialize Spark Session
    spark = SparkSession.builder.appName("TrendingTopicsIntegrationTest").getOrCreate()

    # Initialize TrendAnalyzer
    trend_analyzer = TrendAnalyzer(spark)

    # Use a mock data file for realistic testing
    json_file_path = 'meetups.json'  

    # Read the data from the JSON file
    df = spark.read.option("mode", "DROPMALFORMED").json(json_file_path)

    # Process the data
    df = trend_analyzer.process_data(df)

      # Optionally, apply geographic filtering
    filtered_trends = trend_analyzer.filter_by_location(df, 'city', 'London')

    # Analyze trends with the TrendAnalyzer
    df_1 = trend_analyzer.analyze_trends(filtered_trends, "1 hour", "15 minutes")

    # Get the top N trends
    trends = trend_analyzer.get_ranked_trends(df_1, 5)
    
    # Analyze trends with decay
    df_2 = trend_analyzer.analyze_trends_with_decay(filtered_trends, "1 hour", "15 minutes", 0.1)

    # Get ranked trends
    decayed_trends = trend_analyzer.get_ranked_trends(df_2, 5)

    
    # Output the results for verification
    print("Filtered trends: ", filtered_trends.collect())
    print("Trends ranked: ", trends.collect())
    print("Decayed trends ranked: ", decayed_trends.collect())

    # Stop the Spark Session
    spark.stop()

if __name__ == '__main__':
    integration_test()