"""
etl_job.py
~~~~~~~~~~

This ETL jobs can be submitted to a Spark cluster (or locally) using the 'spark-submit'.
For example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py
spark-submit --py-files packages.zip --files configs/etl_config.json jobs/etl_job.py
where packages.zip contains Python modules required by ETL job;
etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

"""

from dependencies.etlcomponents import *
from dependencies.spark import start_spark


class Executor(object):
    def __init__(self, spark, log, tasks=[], environment='local'):
        self.tasks = tasks
        self.spark = spark
        self.log = log
        self.environment = environment

    def run(self):

        for task in self.tasks:
            if isinstance(task, Extract):
                execute_df = task.execute(self.spark)
                self.log.info("Extract completed")
            if isinstance(task, Transform):
                transf_df = task.execute(self.spark, execute_df)
                self.log.info("Transform completed")
            if isinstance(task, Load):
                task.execute(transf_df)
                self.log.info("Load completed")


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config, environment = start_spark(
        app_name='hello_fresh_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.info('hello_fresh_etl_job is up-and-running')

    # Create ETL Components
    try:
        tasks = [Extract(config['extract'], log), Transform(config['transform'], log), Load(config['load'], log)]
    except KeyError as e:
        print("Some component missing: " + repr(e))

    Executor(spark, log, tasks, environment).run()

    # log the success and terminate Spark application
    log.info('etl_job is finished')
    spark.stop()
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
