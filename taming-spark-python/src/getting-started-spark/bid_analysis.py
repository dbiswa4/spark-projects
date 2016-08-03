from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

'''
Ref:
=> More in reading csv directly
http://stackoverflow.com/questions/28782940/load-csv-file-with-spark
=> For now, let's read it as text file and split based in comma
'''

def analyze_bid_data(sc, sqlContext):
    print 'analyze_bid_data()'


if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local[*]").setAppName("eBay Bid Analysis")
    sparkConf = SparkConf().setAppName("eBay Bid Analysis")
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    analyze_bid_data(sc, sqlContext)