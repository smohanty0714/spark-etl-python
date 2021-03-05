"""
util.py
~~~~~~~~

Module containing Helper Utilities
"""

from dependencies.exception import UdfUnavailable
from dependencies.udf import *


def register_udf(spark, udfs):
    """
    Register Custom UDF's
    :param spark: Spark Session
    :param udfs: UDFs to be registered
    :return:
    """
    available_udfs = ["tominutes"]
    unavailable_udf = [udf for udf in udfs if udf not in available_udfs]
    if len(unavailable_udf) != 0:
        e = "UDF not available: " + ",".join(unavailable_udf)
        raise UdfUnavailable(e)

    for udf in udfs:
        if udf == 'tominutes':
            spark.udf.register("tominutes", tominutes)
            return None


