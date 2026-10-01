from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
spark_session = (SparkSession.builder .appName("IBEX35") .config('spark.driver.extraClassPath', "lib/mysql-connector-j-26.7.0.jar") .getOrCreate()) 


