"""
spark.py
~~~~~~~~
Spark session, get Spark logger and load config files
"""

from json import loads
from os import environ, listdir, path

import __main__

from dependencies import logging
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def start_spark(app_name='hello_fresh_etl_job', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*].
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # detect execution environment
    flag_repl = False if hasattr(__main__, '__file__') else True
    flag_debug = True if 'DEBUG' in environ.keys() else False

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
                .builder
                .appName(app_name)
                )
        environment = 'local'
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
                .builder
                .master(master)
                .appName(app_name)
                )

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if len(config_files) != 0:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_json = config_file.read().replace('\n', '')
        config_dict = loads(config_json)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        try:
            with open('configs/etl_config.json', 'r') as config_file:
                config_json = config_file.read().replace('\n', '')
            config_dict = loads(config_json)
        except FileNotFoundError:
            config_dict = None

    return spark_sess, spark_logger, config_dict, environment

