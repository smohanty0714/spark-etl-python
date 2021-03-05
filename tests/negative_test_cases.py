"""
negative_test_cases.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import json
import unittest

from dependencies.spark import start_spark
from dependencies.etlcomponents import *


class SparkETLTestsNeg(unittest.TestCase):
    """Test suite for transformation in etl_job.py

    *****important**************
    sample data details : { total records: 1,
                            no of records contains beef in ingredient: 1 ,
                            no of records doesn't contain beef in in ingredients: 0
                            }
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark, self.log, *_ = start_spark(
            app_name='unittest_etl_job')

        self.config = json.loads("""{
                                      "extract"  : {"uri": "tests/test_data/udf_test_data/recipes_negative.json",
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

    def test_udf_tominute_neg(self):
        """
        Expectation: What happens to Empty or incorrect Prep or Cook Time
        """

        df_extract = Extract(self.config["extract"]).execute(self.spark)

        df = Transform(self.config["transform"]).execute(self.spark, df_extract)

        df.select('difficulty', 'avg_total_cooking_time').show()


if __name__ == '__main__':
    unittest.main()
