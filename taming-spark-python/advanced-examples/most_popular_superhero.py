from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local").setAppName("Popular Superhero")
    sparkConf = SparkConf().setAppName("Popular Superhero")
    sc = SparkContext(conf=sparkConf)