import unittest
from pyspark.sql import SparkSession, Row
from src.trend_analyzer import TrendAnalyzer
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timedelta

class TestTrendAnalyzer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TrendAnalyzerTests").getOrCreate()
        cls.schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("topic", StringType(), True)
        ])
        cls.trend_analyzer = TrendAnalyzer(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def create_test_dataframe(self, data):
        return self.spark.createDataFrame(data, self.schema)

    def test_analyze_trends(self):
        # Setup test data
        test_data = [
            Row(timestamp=datetime.strptime("2021-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"), topic="Topic A"),
            Row(timestamp=datetime.strptime("2021-01-01T00:30:00", "%Y-%m-%dT%H:%M:%S"), topic="Topic A"),
            Row(timestamp=datetime.strptime("2021-01-01T01:00:00", "%Y-%m-%dT%H:%M:%S"), topic="Topic B"),
            Row(timestamp=datetime.strptime("2021-01-01T02:00:00", "%Y-%m-%dT%H:%M:%S"), topic="Topic B"),
            Row(timestamp=datetime.strptime("2021-01-01T03:00:00", "%Y-%m-%dT%H:%M:%S"), topic="Topic B")
        ]
        test_df = self.create_test_dataframe(test_data)

        # Test analyze_trends method
        analyzed_df = self.trend_analyzer.analyze_trends(test_df, "2 hours", "1 hour")
        result = analyzed_df.collect()

        # Assert the results
        self.assertEqual(len(result), 2)  # Expecting two time windows

    def generate_time_series_data(self, start_time, interval_minutes, count, topic):
            """
            Generates a series of data points spread out over time for a given topic.
            """
            return [Row(timestamp=(start_time + timedelta(minutes=i * interval_minutes)), topic=topic) for i in range(count)]

    def test_analyze_trends_with_decay(self):
        # Setup test data with timestamps that would be affected by decay
        start_time = datetime(2021, 1, 1)        

        # Generate data for two topics with different time distributions
        data_topic_a = self.generate_time_series_data(start_time, 30, 4, "Topic A")  # Every 30 minutes
        data_topic_b = self.generate_time_series_data(start_time, 120, 2, "Topic B")  # Every 2 hours

        # Add more recent data for Topic A to ensure its weighted count is higher
        recent_time = datetime(2021, 1, 2)
        data_topic_a += self.generate_time_series_data(recent_time, 15, 8, "Topic A")  # Every 15 minutes

        test_data = data_topic_a + data_topic_b
        test_df = self.create_test_dataframe(test_data)

        # Test analyze_trends_with_decay method
        decay_factor = 0.1  # Example decay factor
        analyzed_df = self.trend_analyzer.analyze_trends_with_decay(test_df, "2 hours", "1 hour", decay_factor)
        result = analyzed_df.collect()

        # Assert the results
        # The test expects that Topic A, with more frequent recent events, will have a higher weighted count
        for row in result:
            if row["topic"] == "Topic A":
                self.assertTrue(row["weighted_count"] > row["count"])
            elif row["topic"] == "Topic B":
                self.assertTrue(row["weighted_count"] <= row["count"])


                
if __name__ == '__main__':
    unittest.main()