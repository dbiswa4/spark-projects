from pyspark import SparkConf, SparkContext

'''
mav vs mapValues
http://stackoverflow.com/questions/36696326/map-vs-mapvalues-in-spark

Both are same:
val result: RDD[(A, C)] = rdd.map { case (k, v) => (k, f(v)) }

val result: RDD[(A, C)] = rdd.mapValues(f)

The last difference concerns partitioning: if you applied any custom partitioning to your RDD
(e.g. using partitionBy), using map would "forget" that paritioner (the result will revert to default
partitioning) as the keys might have changed; mapValues, however, preserves any partitioner set on the RDD.
'''

def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

def parse_line_other(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return [age, num_friends]


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

    #ToDo
    #Write a file in below format
    #<Age, Total Number of Friends, Number of perosn with this age, Avg no of friends of this age>


    '''
    Experiment with braces
    It did not make a difference here. Is it because of it has two fields in rdd; one is considered key and
    other one is considered value ???
    '''
    print '\n\n***Experiment with braces***\n\n'

    rdd = lines.map(parse_line_other)
    totals_by_age = rdd.mapValues(lambda x:(x, 1)).reduceByKey(lambda x, y:(x[0] + y[0], x[1] + y[1]))
    avg_by_age = totals_by_age.mapValues(lambda x:x[0]/x[1])
    results = avg_by_age.collect()
    print '\ntype of results object : ', type(results), '\n'

    '''
    result is of list type; list of tuples
    '''

    for result in results:
        print '\ntype of result object : ', type(result)
        print result

