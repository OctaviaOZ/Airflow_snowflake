import argparse
import logging

from pyspark.sql.functions import ( 
    col, collect_set, concat_ws, regexp_replace, sort_array, substring
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


class CounterpartyDataProcessor:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = SparkSession.builder.appName("Counterparty Data").getOrCreate()
        self.schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("iban", StringType(), True)
        ])

    # Validate IBAN format
    # Note: This regular expression only checks the structure and length of the IBAN, 
    # not the actual content (e.g. country code, check digits)
    def validate_iban(self, iban_col):
        return iban_col.rlike('^([A-Z]{2})([0-9]{20})') & (col('iban').rlike('^.{22}'))

    # Resolve entities based on bank account and subaccount
    def resolve_entities(self, df):
        return df.filter(col('is_valid')) \
           .withColumn('bank_account', substring(col('iban_clean'), 5, 16).cast(StringType())) \
           .withColumn('subaccount', substring(col('iban_clean'), 21, 2).cast(StringType())) \
           .groupBy('bank_account') \
           .agg(concat_ws(',', sort_array(collect_set('subaccount'))).alias('subaccount_list'),
                concat_ws(',', sort_array(collect_set('name'))).alias('name_list'),
                concat_ws(',', sort_array(collect_set('iban'))).alias('iban_list'))

    # Process the input data
    def process(self):
        try:
            df = self.spark.read.csv(self.input_path, header=True, schema=self.schema)
        except Exception as e:
            logging.error(f"Failed to load input file {self.input_path}: {e}")
            return

        # Clean and validate IBAN
        df_clean = df.withColumn("iban_clean", regexp_replace(col("iban"), " ", ""))
        df_valid = df_clean.withColumn('is_valid', self.validate_iban(col('iban_clean')))
        
        # Resolve entities
        df_resolved = self.resolve_entities(df_valid)

        # Write the final output to a new CSV file
        try:
            df_resolved.coalesce(1).write.csv(self.output_path, header=True, mode="overwrite")
        except Exception as e:
            logging.error(f"Failed to write output file {self.output_path}: {e}")
            return

        logging.info(f"Entity resolution completed. Data saved to {self.output_path}")

    # Stop the SparkSession
    def stop(self):
        self.spark.stop()


if __name__ == '__main__':
    # Set the logging level to INFO
    logging.basicConfig(level=logging.INFO)
    
    # Create a CounterpartyDataProcessor instance and process the data
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Clean and deduplicate counterparty data')
    parser.add_argument('input_path', type=str, help='Path to the input CSV file')
    parser.add_argument('output_path', type=str, help='Path to the output CSV file')
    args = parser.parse_args()

    # Call the main function with the input and output file paths
    # example: python data_engineer_assignment.py source_1_2_2_1.csv output_1_2_2_1.csv
    processor = CounterpartyDataProcessor(args.input_path, args.output_path)
    processor.process()
    processor.stop()