from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row


if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local").setAppName("Total Spent by Customer - Sorted")
    sparkConf = SparkConf().setAppName("Slicing and Dicing Parquet File")
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    input = sqlContext.parquetFile()
    print 'type of input var : ', type(input)