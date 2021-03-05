"""
etlcomponents.py
~~~~~~~~

Module containing ETL components
"""
from pyspark.sql.functions import lower, col ,avg, when
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType

from dependencies.util import *


class Extract(object):
    """
    ETL Extract Component
    """

    def __init__(self, extract_config, log):
        self.uri = extract_config.get('uri')
        self.clean = extract_config.get('clean', True)
        self.log = log

    def execute(self, spark):
        """Load data from json file format.

        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
        df = (
            spark
                .read
                .json(self.uri)
        )

        cleaned_df = self.normalize_column_value(df)

        return cleaned_df

    def normalize_column_value(self, df):
        df_cleaned = df.withColumn('ingredients', F.lower(col('ingredients')))
        return df_cleaned


class Transform(object):
    """
    ETL Transform component
    """

    def __init__(self, transform_config, log):
        self.udfs_required = transform_config.get('udfs_required', ["tominutes"])
        self.ingredient = transform_config.get('ingredient', 'beef')
        self.easy_threshold = transform_config.get('easy_threshold', 30)
        self.medium_threshold = transform_config.get('medium_threshold', 60)
        self.hard_threshold = transform_config.get('hard_threshold', 60)
        self.log = log

    def execute(self, spark, df):
        """Transform original data set.

        :param df:
        :param spark: Spark Session Object
        :param:  DataFrame to be process
        :return: Transformed DataFrame.
        """
        register_udf(spark, self.udfs_required)
        df_filtered = self.extract_recipies_have_ingredients(df, self.ingredient)
        df_transformed = self.get_avg_time_per_difficulty(df_filtered, self.easy_threshold,
                                                          self.medium_threshold, self.hard_threshold)
        return df_transformed

    def extract_recipies_have_ingredients(self, df, ingredient):
        """
        extract only recipes that have beef as one of the ingredients
        :param df: DataFrame to be process
        :param ingredient: String ingredient name
        :return: transformed DataFrame
        """
        df_filtered = df.filter(df.ingredients.contains('beef'))
        return df_filtered

    def get_avg_time_per_difficulty(self, df, easy_threshold, medium_threshold, hard_threshold):
        """
        calculate average cooking time duration per difficulty level
        :param df: DataFrame to be process
        :param easy_threshold: Integer easy threshold
        :param medium_threshold: Integer medium threshold
        :param hard_threshold: Integer hard threshold
        :return: transformed DataFrame
        """

        df_filtered_min = df.withColumn('cookTime_in_Min', tominutes(col('cookTime')).cast(IntegerType())) \
            .withColumn('prepTime_in_Min', tominutes(col('prepTime')).cast(IntegerType()))

        df_filtered_with_time = df_filtered_min. \
            withColumn("total_time", (col("cookTime_in_Min") + col("prepTime_in_Min")).cast(
                                                               IntegerType())). \
            withColumn("difficulty",
                       when(col("total_time") < easy_threshold, 'easy').
                       when((col("total_time") >= easy_threshold) & (col("total_time") <= medium_threshold), 'medium').
                       when(col("total_time") > hard_threshold, 'hard')
                       .cast(StringType()))

        result_df = df_filtered_with_time.groupBy(col('difficulty')). \
            agg(avg(col('total_time')).cast(DecimalType(38, 2)).alias("avg_total_cooking_time"))

        return result_df


class Load(object):
    """
    ETL Load Component
    """

    def __init__(self, load_config, log):
        self.load_path = load_config.get('load_path', '/output/report.csv')
        self.log = log

    def execute(self, df):
        """Load Executor
        :param df: DataFrame to save as result
        :return: None
        """
        # save to disk
        df.toPandas().to_csv(self.load_path, index=False, header=True)

        return None
