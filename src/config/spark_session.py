import os
from pyspark.sql import SparkSession

def get_spark_session(): #(Se utilizó la ayuda de gemini para esta función)
    config_dir = os.path.dirname(os.path.abspath(__file__))
    
    base_dir = os.path.dirname(config_dir)
    
    jar_name = "mysql-connector-j-26.7.0.jar" 
    path_jar = os.path.join(base_dir, "drivers", jar_name)

    spark = (SparkSession.builder
             .appName("IBEX35")
             .config("spark.driver.extraClassPath", path_jar)
             .config("spark.jars", path_jar)
             .getOrCreate())
             
    spark.sparkContext.setLogLevel("ERROR")

    return spark