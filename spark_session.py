import os
from pyspark.sql import SparkSession

def get_spark_session():
    dir_actual = os.path.dirname(os.path.abspath(__file__))

    nombre_jar = "mysql-connector-j-26.7.0.jar"
    path_jar = os.path.join(dir_actual, nombre_jar)

    spark = (SparkSession.builder
             .appName("IBEX35")
             .config("spark.driver.extraClassPath", path_jar)
             .config("spark.jars", path_jar)
             .getOrCreate())
             
    spark.sparkContext.setLogLevel("ERROR")

    return spark