from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

'''
https://spark.apache.org/docs/1.6.1/sql-programming-guide.html
#Schema Merging
1. setting data source option mergeSchema to true when reading Parquet files (as shown in the examples below), or
2. setting the global SQL option spark.sql.parquet.mergeSchema to true.
'''

def schema_mering_example(sqlContext):
    print 'Schema Mering Example'
    # Create a simple DataFrame, stored into a partition directory
    df1 = sqlContext.createDataFrame(sc.parallelize(range(1, 6)) \
                                     .map(lambda i: Row(single=i, double=i * 2)))
    df1.write.parquet("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/schema.merging/key=1")

    df1.show()

    # Create another DataFrame in a new partition directory,
    # adding a new column and dropping an existing column
    df2 = sqlContext.createDataFrame(sc.parallelize(range(6, 11))
                                     .map(lambda i: Row(single=i, triple=i * 3)))
    df2.write.parquet("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/schema.merging/key=2")

    df2.show()
    # Read the partitioned table
    df3 = sqlContext.read.option("mergeSchema", "true").parquet("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/schema.merging")
    df3.printSchema()


if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster('local[*]').setAppName('Diffrent Data Source Demo')
    sparkConf = SparkConf().setAppName('Diffrent Data Source Demo')
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    schema_mering_example(sqlContext)
