from pyspark import SparkConf, SparkContext

def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

if __name__ == '__main__':
    sparkConf = SparkConf().setAppName("Average Friends by Age")
    sc = SparkContext(conf=sparkConf)

    '''
    file:/// indicates local file system
    lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
    lines is of pyspark.rdd.RDD type
    '''
    lines = sc.textFile("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/fakefriends.csv")
    print '\ntype of lines object : ', type(lines)

    '''
    rdd is of pyspark.rdd.PipelinedRDD type
    Note:
    => What is pyspark.rdd.PipelinedRDD?
    '''
    rdd = lines.map(parse_line)
    print '\ntype of rdd object : ', type(rdd)

    totals_by_age = rdd.mapValues(lambda x:(x, 1)).reduceByKey(lambda x, y:(x[0] + y[0], x[1] + y[1]))
    avg_by_age = totals_by_age.mapValues(lambda x:x[0]/x[1])
    results = avg_by_age.collect()
    print '\ntype of results object : ', type(results), '\n'

    '''
    result is of list type; list of tuples
    '''

    for result in results:
        print 'type of result object : ', type(result)
        print result