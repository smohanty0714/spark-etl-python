"""
udf.py
~~~~~~~~

Module containing User Defined Functions
"""
import re

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

@udf(returnType=IntegerType())
def tominutes(s):
    """
    convert prepTime and cookTime in customized format to minutes
    :param s: prepTime or Cooktime in helloFresh format
    :return: minutes
    """
    if re.findall(r'^(PT|CT)\d*H*\d*M$', s):
        return eval(s.replace("PT", "").replace("CT", "").replace("M", "*1").replace("H", "*60+"))
    elif re.findall(r'^(PT|CT)\d*H$', s):
        return eval(s.replace("PT", "").replace("CT", "").replace("H", "*60"))

