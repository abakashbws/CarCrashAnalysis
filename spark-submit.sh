#!/bin/bash
export SPARK_HOME="$PWD/spark/spark-3.5.0-bin-hadoop3"  # Adjust the version if needed
export PYSPARK_PYTHON="python3"  # Use python3 or the path to your Python executable
export PYSPARK_DRIVER_PYTHON="python3"  # Use python3 or the path to your Python executable

"$SPARK_HOME"/bin/spark-submit --master local[4] --py-files spark_setup.py main.py --config config/config.json 

