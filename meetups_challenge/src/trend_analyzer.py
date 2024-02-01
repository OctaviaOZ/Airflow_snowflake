# Standard library imports
import os
import logging

# Third-party library imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    desc, 
    date_format, 
    explode, 
    expr, 
    from_unixtime, 
    rank, 
    row_number, 
    udf, 
    window
)
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec
import matplotlib.pyplot as plt
import seaborn as sns


class TrendAnalyzer:
    def __init__(self, spark_session: SparkSession):
        """Initialize the TrendAnalyzer with a SparkSession.""" 
        self.spark = spark_session
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def process_data(self, raw_data):
        """Process the data"""
        try:
            # Extract the necessary fields
            parsed_data = raw_data.filter(raw_data.response == 'yes')\
                .select("group.group_topics.topic_name"\
                , "mtime", "group.group_city", "group.group_country"\
                , "event.event_id", "event.event_name")

            # Convert the 'mtime' field to a timestamp
            parsed_data = parsed_data.withColumn('timestamp'\
                            , from_unixtime(col('mtime') / 1000).cast('timestamp'))
        
            # Process valid data
            # Process with Word2Vec
            # Create a new DataFrame with "topic" and "event_name" columns
            exploded_topics = parsed_data.select(explode(parsed_data.topic_name)\
                                                 .alias("topic"), "event_name", "event_id")
            # Remove duplicates
            exploded_topics = exploded_topics.dropDuplicates(["topic", "event_name", "event_id"])

            word2vec_data = self.process_with_word2vec(exploded_topics)

            parsed_data = parsed_data.select(["topic_name", "group_city"\
                                    , "group_country","event_id","timestamp"]).dropDuplicates()

            processed_data = parsed_data.join(word2vec_data, ["event_id"], "inner")

            # test filter because of the limited and unbalanced data
            # processed_data = processed_data.filter(date_format(col("timestamp")\
            #                                                   , "yyyy-MM-dd") == "2017-03-19")
           
            return processed_data
        except Exception as e:
            self.logger.error(f"Error in process_data: {e}")
            raise
        
    def analyze_trends(self, data, window_duration: str, sliding_interval: str) -> None:
        """Analyze trends in the data."""
        try:
            # withWatermark() defines the maximum delay in processing the data
            # we don't have stream data
            windowed_df = data.groupBy(
                                window(col("timestamp"), window_duration, sliding_interval),
                                col("topic")
                            ).count()
            return windowed_df
        except Exception as e:
            self.logger.error(f"Error in analyze_trends: {e}")
            return None 

    def get_ranked_trends(self, data, N: int) -> None:
        """Rank the topics by count."""
        try:
            window = Window.partitionBy("window").orderBy(desc("count"))
            return data.withColumn("rank", rank().over(window)) \
                        .filter(col("rank") <= N) \
                        .drop("rank")
        except Exception as e:
            self.logger.error(f"Error in get_ranked_trends: {e}")
            return None      

    def analyze_trends_with_decay(self, data, window_duration: str\
                                  , sliding_interval: str, decay_factor: float) -> None:
        """Apply a decay factor to the counts based on the time difference."""
        try:
            windowed_df = data.groupBy(
                                window(col("timestamp"), window_duration, sliding_interval),
                                col("topic")
                            ).count()

            decayed_df = windowed_df.withColumn("decay", 
                        expr(f"exp(-{decay_factor} \
                        * (unix_timestamp(current_timestamp()) - unix_timestamp(window.end)))")) \
                        .withColumn("weighted_count", col("count") * col("decay"))
            return decayed_df
        except Exception as e:
            self.logger.error(f"Error in analyze_trends_with_decay: {e}")
            return None

    def filter_by_location(self, data, location_type: str, location_name: str) -> None:
        """Filter the DataFrame by a given location type and name."""
        try:
            if location_type not in ['city', 'country']:
                raise ValueError("location_type must be 'city' or 'country'")
            location_column = f"group_{location_type}"
            return data.filter(col(location_column) == location_name)
        except Exception as e:
            self.logger.error(f"Error in filter_by_location: {e}")
            return None
        
    def plot_data(self, data, filter_name: str = None, filter_value: str = None):
        """Plot the data"""
        try:
            # Convert the DataFrame to Pandas and plot it
            pd_df = data.toPandas()
            pd_df['time'] = pd_df['window'].apply(lambda x: x['start'].strftime('%H:%M'))
            sns.set_style("whitegrid")
            plt.figure(figsize=(11,7))
            sns.set(font_scale=0.8)

            if filter_name and filter_value:
                plt.title(f'Count of Topics Over Time for {filter_name} = {filter_value}')
            else:
                plt.title('Count of Topics Over Time')

            p = sns.barplot(x='time', hue="topic", y='count',data=pd_df)
            _ = p.set(xlabel="time", ylabel="Count")
            plt.legend(loc='upper left')
            plt.grid(True) 
            plt.show()
        except Exception as e:
            self.logger.error(f"Error in plot_data: {e}")
        return None

    def analyze_and_plot(self, data, window_duration, slide_duration, n\
                         , filter_name=None, filter_value=None):
        '''Analyze trends in the data and plot the results.'''
        window_data = self.analyze_trends(data, window_duration, slide_duration)
        plot_data = self.get_ranked_trends(window_data, n)
        _ = self.plot_data(plot_data, filter_name, filter_value)  

    def process_column_with_word2vec(self, data, inputCol, outputCol):
        # Tokenize the column
        tokenizer = Tokenizer(inputCol=inputCol, outputCol=f"{inputCol}_words")
        wordsData = tokenizer.transform(data)

        # Remove stop words
        remover = StopWordsRemover(inputCol=f"{inputCol}_words", outputCol=f"filtered_{inputCol}_words")
        wordsData = remover.transform(wordsData)

        # Train a Word2Vec model
        word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol=f"filtered_{inputCol}_words"\
                            , outputCol=f"{outputCol}_features")
        model = word2Vec.fit(wordsData)
        wordsData = model.transform(wordsData)

        return wordsData      

    def process_with_word2vec(self, data):
        # Check if wordsData is cached
        if os.path.exists('wordsData.parquet'):    
            self.logger.info("Using cached DataFrame for word2vec")
            wordsData = self.spark.read.parquet('wordsData.parquet')
        else:
            self.logger.info("Processing word2vec")

            # Process the event_name and topic columns with Word2Vec
            wordsData = self.process_column_with_word2vec(data, "event_name", "event")
            wordsData = self.process_column_with_word2vec(wordsData, "topic", "topic")

            # Define a UDF to compute cosine similarity
            def cosine_similarity(a, b):
                return float(a.dot(b) / (a.norm(2) * b.norm(2)))

            cosine_similarity_udf = udf(cosine_similarity, FloatType())

            # Remove redundant columns
            wordsData = wordsData.drop('event_words', 'event_name', 'topic_words'\
                                       , 'filtered_event_words', 'filtered_topic_words')

            # Compute cosine similarity between event_name and topic vectors
            wordsData = wordsData.withColumn("similarity"\
                                , cosine_similarity_udf(col("event_features"), col("topic_features")))

            # Define a window partitioned by event_id, ordered by similarity
            window = Window.partitionBy("event_id").orderBy(desc("similarity"))

            # Add a row_number for each window and filter the top row
            wordsData = wordsData.withColumn("rn", row_number().over(window)) \
                                .filter(col("rn") == 1) \
                                .drop("rn")

            # Remove redundant columns
            wordsData = wordsData.drop('event_features', 'topic_features', 'similarity')

            # Write the DataFrame to a Parquet file
            wordsData.write.parquet('wordsData.parquet')

        return wordsData