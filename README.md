# Sample ETL Spark Job using Python and PySpark

This document is describing the project structure and local env setup of the ETL job for Spark JOB.

## Project Structure

The project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- logging.py
 |   |-- etlcomponents.py
 |   |-- exception.py
 |   |-- spark.py
 |   |-- udf.py
 |   |-- util.py
 |-- jobs/
 |   |-- etl_job.py
 |-- output/
 |   |--report.csv
 |-- tests/
 |   |-- test_data/
 |   |-- | -- recipes/
 |   |-- |    |-- customer.csv
 |   |-- | -- recipes/
 |   |-- |    |-- recipes_negative.json
 |   |-- |    |-- recipes_positive.json
 |   |-- negative_test_cases.py
 |   |-- positive_test_cases.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```

The main Python module containing the ETL job (which will be sent to the Spark cluster), is `jobs/etl_job.py`. the transformation logic is on `/dependencies/*` .  Any external configuration parameters required by `etl_job.py` are stored in JSON format in `configs/etl_config.json`. Additional modules that support this job can be kept in the `dependencies` folder (more on this later). In the project's root we include `build_dependencies.sh`, which is a bash script for building these dependencies into a zip-file to be sent to the cluster (`packages.zip`). 

Unit test modules are kept in the `tests` folder and All master data present on `tests\test-data`.

## Running the ETL job

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,

1. ``pyspark,pandas `` python package need to be install 
2.  Go to project home and run``./build_dependencies.sh `` 
3.  For local env can directly submit :

```bash
$SPARK_HOME/bin/spark-submit \
--py-files packages.zip --files configs/etl_config.json jobs/etl_job.py
```

If you wish submit to cluster the configuration then follow this approach of submitting :

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--packages 'com.somesparkjar.dependency:1.0.0' \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py
```

Briefly, the options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--packages 'com.somesparkjar.dependency:1.0.0,...'` - Maven coordinates for any JAR dependencies required by the job (e.g. JDBC driver for connecting to a relational database);
- `--files configs/etl_config.json` - the (optional) path to any config file that may be required by the ETL job;
- `--py-files packages.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_job.py` - the Python module file containing the ETL job to execute.

Full details of all possible options can be found [here](http://spark.apache.org/docs/latest/submitting-applications.html). Note, that we have left some options to be defined within the job (which is actually a Spark application) - e.g. `spark.cores.max` and `spark.executor.memory` are defined in the Python script as it is felt that the job should explicitly contain the requests for the required cluster resources.

## Checking the output

After successful completion of the Job, Result will be stored in ``output/report.csv`` 


## Passing Configuration Parameters to the ETL Job

Although it is possible to pass arguments to `etl_job.py`, as you would for any generic Python module running as a 'main' program  - by specifying them after the module's filename and then parsing these command line arguments - this can get very complicated, very quickly, especially when there are lot of parameters (e.g. credentials for multiple databases, table names, SQL snippets, etc.). This also makes debugging the code from within a Python interpreter extremely awkward, as you don't have access to the command line arguments that would ordinarily be passed to the code, when calling it from the command line.

A much more effective solution is to send Spark a separate file - e.g. using the `--files configs/etl_config.json` flag with `spark-submit` - containing the configuration in JSON format, which can be parsed into a Python dictionary in one line of code with `json.loads(config_file_contents)`. Testing the code from within a Python interactive console session is also greatly simplified, as all one has to do to access configuration parameters for testing, is to copy and paste the contents of the file - e.g.,

```python
import json

config = json.loads("""{"field": "value"}""")
```

For the exact details of how the configuration file is located, opened and parsed, please see the `start_spark()` function in `dependencies/spark.py` (also discussed further below), which in addition to parsing the configuration file sent to Spark (and returning it as a Python dictionary), also launches the Spark driver program (the application) on the cluster and retrieves the Spark logger at the same time.

## Packaging ETL Job Dependencies

In this project, functions that can be used across different ETL jobs are kept in a module called `dependencies` and referenced in specific job modules using, for example,

```python
from dependencies.spark import start_spark
```

This package, together with any additional dependencies referenced within it, must be copied to each Spark node for all jobs that use `dependencies` to run. This can be achieved in one of several ways:

1. send all dependencies as a `zip` archive together with the job, using `--py-files` with Spark submit;
2. formally package and upload `dependencies` to somewhere like the `PyPI` archive (or a private version) and then run `pip3 install dependencies` on each node; or,
3. a combination of manually copying new modules (e.g. `dependencies`) to the Python path of each node and using `pip3 install` for additional dependencies (e.g. for `requests`).

Option (1) is by far the easiest and most flexible approach, so we will make use of this for now. To make this task easier, especially when modules such as `dependencies` have additional dependencies (e.g. the `requests` package), we have provided the `build_dependencies.sh` bash script for automating the production of `packages.zip`, given a list of dependencies documented in `Pipfile` and managed by the `pipenv` python application (discussed below).


## Automated Testing

To execute the unit test for this project run.

```bash
pipenv run python -m unittest tests/test_*.py
```

