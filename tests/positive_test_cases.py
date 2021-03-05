"""
positive_test_cases.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import json
import unittest
import sys

from dependencies.spark import start_spark
from dependencies.etlcomponents import *


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py

    **************************************IMPORTANT TO KNOW**************************************************
    sample data details : { total records: 5,
                            no of records contains beef in ingredient: 3 (first 3 record),
                            no of records doesn't contain beef in in ingredients: 1 (last but one record),
                            no of records containing non-printable characters: 1(last record)
                            }
    *********************************************************************************************************
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark, *_ = start_spark(
            app_name='unittest_etl_job')

        self.config = json.loads("""{
                                      "extract"  : {"uri": "tests/test_data/udf_test_data/recipes_positive.json",
                                                    "clean": "True"},
                                      "transform": {"udfs_required":["tominutes"],
                                                    "ingredient": "beef",
                                                    "ingredient": 30,
                                                    "ingredient": 60,
                                                    "ingredient": 60
                                      },
                                      "load"     : {
                                                    "load_path": "output/report.csv"
                                                    }
                                    }
                            
                            """)

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_udf_tominute(self):
        """Testing UDF tominute
        Expectation: PT15M + PT6M = 21 minutes
        """

        df_extract = Extract(self.config["extract"]).execute(self.spark)

        df = Transform(self.config["transform"]).execute(self.spark, df_extract)

        result = df.select('total_time', 'prepTime_in_Min', 'cookTime_in_Min', 'difficulty').collect()

        result_list = [(row['total_time'], row['prepTime_in_Min'], row['cookTime_in_Min'], row['difficulty']) for row in result]

        expected_list = [(21.0, 'PT15M', 'PT6M', 'easy'), (16.0, 'PT6M', 'PT10M', 'easy'),
                         (20.0, 'PT5M', 'PT15M', 'easy'), (165.0, 'PT2H', 'PT45M', 'hard')]

        self.assertEqual(result_list, expected_list)

    def test_extract(self):
        """Testing Extract
        Expectation: Should Load all the records
        """

        df = Extract(self.config["extract"]).execute(self.spark)

        ext_rec_count = df.count()
        exp_rec_count = 5

        self.assertEqual(ext_rec_count, exp_rec_count)

    def test_transform(self):
        """Testing transform

        Expectation: should load only records contains "beef" (refer configs/transformation.sql)
        """

        df_extract = Extract(self.config["extract"]).execute(self.spark)
        df = Transform(self.config["transform"]).execute(self.spark, df_extract)

        ext_rec_count = df.count()
        exp_rec_count = 4

        self.assertEqual(ext_rec_count, exp_rec_count)


if __name__ == '__main__':
    unittest.main()
