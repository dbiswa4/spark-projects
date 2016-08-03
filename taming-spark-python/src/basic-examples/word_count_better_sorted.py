from pyspark import SparkContext, SparkConf
import re

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

if __name__ == '__main__':
    #sparkConf = SparkConf().setAppName("Word Count Better - Sorted")
    sparkConf = SparkConf().setMaster("local").setAppName("Word Count Better - Sorted")
    sc = SparkContext(conf=sparkConf)

    input = sc.textFile("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/story.txt")
    words = input.flatMap(normalizeWords)

    #Note:
    #map(lambda x: (x,1))
    #If you give only x, 1 without the braces, it will not work
    #
    #reduceByKey(lambda x, y: (x + y))
    #It works with or without braces

    word_counts = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: (x + y))

    #Note:
    #See the difference of lambda function passed in reducedByKey() method above and map() method below
    #In first case, for same key all the values was being passed one by one and we added one value to the next
    #In later case, each line is being operated at one in map() method and (x, y) indicates the fields of each record
    #Question:
    #If x, y indicates the fields in each line, how spark knows of the separator?

    word_counts_sorted = word_counts.map(lambda (x,y):(y,x)).sortByKey()
    print '\nword_counts_sorted : ', type(word_counts_sorted), '\n'

    results = word_counts_sorted.collect()
    print '\nresults : ', type(results), '\n'

    #results = word_counts.collect()

    for result in results:
        print 'result type : ', type(result)
        print result
        count = str(result[0])
        word = result[1].encode('ascii', 'ignore')
        if(word):
            print word + " : " + count





