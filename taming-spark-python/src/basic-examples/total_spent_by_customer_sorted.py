from pyspark import SparkConf, SparkContext


def extractCustomerPricePairs(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local").setAppName("Total Spent by Customer - Sorted")
    sparkConf = SparkConf().setAppName("Total Spent by Customer - Sorted")
    sc = SparkContext(conf=sparkConf)

    input = sc.textFile("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/customer-orders.csv")
    print 'input type : ', type(input)

    mappedInput = input.map(extractCustomerPricePairs)
    totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
    print 'totalByCustomer type : ', type(totalByCustomer)

    flipped = totalByCustomer.map(lambda (x, y): (y, x))
    totalByCustomerSorted = flipped.sortByKey()
    print 'totalByCustomerSorted type : ', type(totalByCustomerSorted)

    results = totalByCustomerSorted.collect()
    print 'results type ', type(results)

    for result in results:
        print result
        print 'result type : ', type(result)
        print 'spent : ' + str(round(result[0], 2)) + ' cust id : ' + str(result[1])
