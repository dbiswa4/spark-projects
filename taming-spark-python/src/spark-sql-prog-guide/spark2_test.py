from pyspark.sql import SparkSession
from pyspark.sql import *

'''
Error:
It throws below error.

jsparkSession = self._jvm.SparkSession(self._jsc.sc())
TypeError: 'JavaPackage' object is not callable
'''

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:/Users/dbiswas/Documents/Software/Spark/spark-workspace").appName("SparkSQL").getOrCreate()


