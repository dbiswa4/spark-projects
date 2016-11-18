from pyspark import SparkConf, SparkContext

def convert_with_round(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    return (heroID, (connections, distance, color))

def convert_with_square(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    '''
    If you don't use extra pair of [] around inside array, basically it generate the same effect as using
    the round braces
    '''
    return [heroID, [[connections, distance, color]]]


def create_starting_rdd_round():
    inputFile = sc.textFile('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/braces-test.txt')
    return inputFile.map(convert_with_round)

def create_starting_rdd_square():
    inputFile = sc.textFile('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/braces-test.txt')
    return inputFile.map(convert_with_square)

def round_brace_experiment():

    '''
    Input:
    5988 777 666 444 555
    5988 9999 5555 4444 3333

    Below code will produce following output:

    (5988, ([777, 666, 444, 555], 9999, 'WHITE', [9999, 5555, 4444, 3333], 9999, 'WHITE'))

    The output is not helpful at all as it mixes the fields from different values for same key?

    How can this be avoided? May be use a better function in reduceByKey() operation?
    '''

    rdd = create_starting_rdd_round()
    for row in rdd.collect():
        print '\n', row

    print '\n\nAggregated rdd:\n'
    rdd_aggr = rdd.reduceByKey(lambda a,b : a + b )

    for row in rdd_aggr.collect():
        print '\n', row

def square_brace_experiment():

    '''
    Input:
    5988 777 666 444 555
    5988 9999 5555 4444 3333

    Below code will produce following output:

    (5988, [[[777, 666, 444, 555], 9999, 'WHITE'], [[9999, 5555, 4444, 3333], 9999, 'WHITE']])

    This is still fine as we are able to distinguish different values for same key
    '''

    rdd = create_starting_rdd_square()
    for row in rdd.collect():
        print '\n', row

    print '\n\nAggregated rdd:\n'
    rdd_aggr = rdd.reduceByKey(lambda a,b : a + b )

    for row in rdd_aggr.collect():
        print '\n', row


def process_values(a, b):
    print '\nI am in process_values()'
    print a
    print b


def round_brace_experiment_enhanced():
    rdd = create_starting_rdd_round()
    for row in rdd.collect():
        print '\n', row

    '''
    Below example does not work !!! why?
    Workaround is reduceByKey(lambda a,b : a + b ).mapValues(custom_function)
    '''
    print '\n\nAggregated rdd:\n'
    rdd_aggr = rdd.reduceByKey(process_values)

    #for row in rdd_aggr.collect():
    #    print '\n', row


if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local").setAppName("Degrees of Separation")
    sparkConf = SparkConf().setAppName("Degrees of Separation")
    sc = SparkContext(conf=sparkConf)

    #round_brace_experiment()

    #square_brace_experiment()

    round_brace_experiment_enhanced()



