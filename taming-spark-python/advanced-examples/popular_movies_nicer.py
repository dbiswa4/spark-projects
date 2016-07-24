from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/u.item') as f:
        for line in f:
            fields = line.split('|')
            #populated dictionary which is initialized above
            movieNames[int(fields[0])] = fields[1]
    return movieNames


if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local").setAppName("Popular Movies")
    sparkConf = SparkConf().setAppName("Popular Movies")
    sc = SparkContext(conf=sparkConf)

    nameDict = sc.broadcast(loadMovieNames())

    lines = sc.textFile('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/u.data')
    '''
        (int(x.split()[1]), 1)
        => it should be wrapped by (), else it will throw below error
        for k, v in iterator:
        TypeError: 'int' object is not iterable
    '''
    movies = lines.map(lambda x:(int(x.split()[1]), 1))
    movieCounts = movies.reduceByKey(lambda x, y: x +y)

    flipped = movieCounts.map(lambda (x, y): (y, x))
    sortedMovies = flipped.sortByKey()

    sortedMoviesWithNames = sortedMovies.map(lambda (count, movie): ((nameDict.value[movie]), count))

    results = sortedMoviesWithNames.collect()

    for result in results:
        print result
        print 'Movie Name : ' + result[0] + " ### Number of times watched : " + str(result[1])

