from pyspark import SparkConf, SparkContext

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/Marvel-Graph.txt')
    return inputFile.map(convertToBFS)

if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local").setAppName("Degrees of Separation")
    sparkConf = SparkConf().setAppName("Degrees of Separation")
    sc = SparkContext(conf=sparkConf)

    # The characters we wish to find the degree of separation between:
    startCharacterID = 5306  # SpiderMan
    targetCharacterID = 14  # ADAM 3,031 (who?)

    # Our accumulator, used to signal when we find the target character during
    # our BFS traversal.
    hitCounter = sc.accumulator(0)

    # Main program here:
    iterationRdd = createStartingRdd()

    for iteration in range(0, 10):
        print "Running BFS iteration# " + str(iteration + 1)

