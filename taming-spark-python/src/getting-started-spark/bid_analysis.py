from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

'''
Ref:
=> More in reading csv directly
http://stackoverflow.com/questions/28782940/load-csv-file-with-spark
=> For now, let's read it as text file and split based in comma
'''

def bid_data_quick_look(sc):
    print 'analyze_bid_data()'
    ebay_text = sc.textFile("/Users/dbiswas/Documents/misc/public_datasets/ebay/Cartier+7-day+auctions.csv")
    print 'type of ebay_text : ', type(ebay_text)
    print 'rows : ', ebay_text.first()
    print 'ebay_text.first() return type : ', type(ebay_text.first())

    ebay = ebay_text.filter(lambda line : "auctionid" not in line).map(lambda line : line.split(",")) \
        .map(lambda line : (int(line[0]), int(line[1]), float(line[2]), line[3].encode("utf8"), \
                            int(line[4]), int(line[5]), int(line[6])))
    print 'type of ebay', type(ebay)
    print 'rows : ', ebay.first()


def bid_data_analysis(sc, sqlContext):
    print '\n In bid_data_analysis()...'
    ebay_text = sc.textFile("/Users/dbiswas/Documents/misc/public_datasets/ebay/Cartier+7-day+auctions.csv")
    ebayTemp = ebay_text.filter(lambda line: "auctionid" not in line).map(lambda line: line.split(","))

    print 'First row of ebayTemp : ', ebayTemp.first()
    print 'type of ebayTemp : ', type(ebayTemp)

    ebay = ebayTemp.map(lambda line : (int(line[0]), float(line[1]), float(line[2]), line[3].encode("utf8"), \
                            float(line[4]), float(line[5]), float(line[6])))

    '''
    +---------+----+-------+-----------------+----------+-------+-----+
    |auctionid| bid|bidtime|           bidder|bidderrate|openbid|price|
    +---------+----+-------+-----------------+----------+-------+-----+
    |     null|null|   null|        kona-java|      null|   null| null|
    |     null|null|   null|           doc213|      null|   null| null|

    Note:
    If we create RDD as shown below, all the fields will be emitted as String. When you then try to attach the schema
    shown below, type cast from String to Integer will fail.
    '''
    #ebay = ebayTemp.map(lambda line : (line[0], line[1], line[2], line[3], \
    #                        line[4], line[5], line[6]))


    '''
    ToDo:
    => How can we store this schema as a separate file and use it in this program to make it reusable?

    '''
    print '\nFirst row of ebay : ', ebay.first()
    print 'type of ebay : ', type(ebay)

    schema = StructType([
        StructField("auctionid", IntegerType()),
        StructField("bid", FloatType()),
        StructField("bidtime", FloatType()),
        StructField("bidder", StringType()),
        StructField("bidderrate", FloatType()),
        StructField("openbid", FloatType()),
        StructField("price", FloatType()),
    ])

    '''
    Spark SQL supports automatically converting an "RDD containing case classes" to a DataFrame with the method toDF():
    // change ebay RDD of Auction objects to a
    val auction = ebay.toDF()

    Let's do it in Python way here.
    '''
    auction = sqlContext.createDataFrame(ebay, schema)

    #Shows first 20 rows
    auction.show()
    #Print Schema
    auction.printSchema()

    auction.select(auction['auctionid']).show()

    '''
    auction.select(auction['auctionid']).distinct.count()
    => It does not work in Python. Looks like SQL APIs are not strong in Python
    => It works in Scala
    => Let's run direct SQL
    '''

    auction.registerTempTable("auction")

    a = sqlContext.sql("select auctionid from auction limit 5")
    a.show()

    a = sqlContext.sql("select distinct auctionid from auction limit 5")
    a.show()

    a = sqlContext.sql("select count(*) from auction")
    a.show()

    a = sqlContext.sql("select count(auctionid) from auction")
    a.show()

    a = sqlContext.sql("select count(distinct auctionid) from auction")
    a.show()

    a = sqlContext.sql("select count(*) from (select distinct auctionid from auction) a")
    a.show()

    '''
    Let's go back to DataFrame methods
    Scala Reference:
    // How many bids per item?  ** There is no column item in data. Seems to be typo
    auction.groupBy("auctionid", "item").count.show

    // What's the min number of bids per item?
    // what's the average? what's the max?
    auction.groupBy("item", "auctionid").count
    .agg(min("count"), avg("count"),max("count")).show
    '''
    #How many bid put by a user in one auction process?
    results = sqlContext.sql("SELECT auctionid, bidder, count(bid) FROM auction GROUP BY auctionid, bidder")
    results.show()

    #auctionid=1638843936
    results = sqlContext.sql("SELECT auctionid, MAX(price) FROM auction GROUP BY auctionid")
    results.show()

    '''
    !!! Looks like MIN() does not work !!!
    '''
    results = sqlContext.sql("SELECT auctionid, MIN(price) FROM auction GROUP BY auctionid")
    results.show()






if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local[*]").setAppName("eBay Bid Analysis")
    sparkConf = SparkConf().setAppName("eBay Bid Analysis")
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    #bid_data_quick_look(sc)
    bid_data_analysis(sc, sqlContext)