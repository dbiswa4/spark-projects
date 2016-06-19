from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import collections

conf = SparkConf().setMaster("local").setAppName("SparkSQL")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

def mapper(line):
    fields = line.split(',')
    return Row( ID=int(fields[0]), name=fields[1].encode("utf-8"), age=int(fields[2]), numFriends=int(fields[3]) )

lines = sc.textFile("fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)
