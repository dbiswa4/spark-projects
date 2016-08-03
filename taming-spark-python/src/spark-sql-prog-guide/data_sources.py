from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

'''
Ref:
https://github.com/apache/spark/blob/master/examples/src/main/python/sql/datasource.py
and
https://spark.apache.org/docs/1.6.1/sql-programming-guide.html
(Data Sources Section)
'''

def parquet_example(sqlContext):
    peopleDF = sqlContext.read.json("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/people.json")

    '''
    Looks like, above read method rearranges the fields in json in alphabetical order.
    Eventhough 'age' is second field in json file, it appears to be the first field in DataFrame
    '''
    print '\n**values after reading from json'
    for p in peopleDF.collect():
        print p
        print 'Name : ' + str(p[0]) + ' and age : ' + p[1]
        print 'Name : ' + p['name'] + ' and age : ' + str(p['age'])


    # DataFrames can be saved as Parquet files, maintaining the schema information.
    peopleDF.write.parquet("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/people.parquet")

    # The result of loading a parquet file is also a DataFrame.
    parquetFile = sqlContext.read.parquet("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/people.parquet")

    print '\n**values after reading from parquet'
    for p in parquetFile.collect():
        print p
        print 'Name : ' + str(p[0]) + ' and age : ' + p[1]
        print 'Name : ' + p['name'] + ' and age : ' + str(p['age'])

    #Run SQL on files directly
    df = sqlContext.sql("SELECT * FROM parquet.`/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/people.parquet`")

    print '\n**values after reading directly from parquet'
    for p in df.collect():
        print p
        print 'Name : ' + str(p[0]) + ' and age : ' + p[1]
        print 'Name : ' + p['name'] + ' and age : ' + str(p['age'])

if __name__ == '__main__':
    sparkConf = SparkConf().setMaster('local[*]').setAppName('Diffrent Data Source Demo')
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    parquet_example(sqlContext)
