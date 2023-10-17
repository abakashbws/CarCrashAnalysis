from pyspark.sql import SparkSession
import os


def setup_spark():
    SPARK_HOME = "C:/spark/spark-3.5.0-bin-hadoop3"  # Update this path to your Spark installation directory
    os.environ["PATH"] = os.path.join(SPARK_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
    
    return SparkSession.builder.appName("CarCrashAnalysis").getOrCreate()
