from pyspark import SparkConf, SparkContext

'''
https://spark.apache.org/docs/1.6.1/programming-guide.html
#
PySpark can also read any Hadoop InputFormat or write any Hadoop OutputFormat, for both ‘new’ and ‘old’
Hadoop MapReduce APIs. If required, a Hadoop configuration can be passed in as a Python dict. Here is an
example using the Elasticsearch ESInputFormat:

#Put ElasticSearch jar at classpath
SPARK_CLASSPATH=/path/to/elasticsearch-hadoop.jar ./bin/pyspark
'''

if __name__ == '__main__':
    sparkConf = SparkConf().setAppName("Read from ElasticSearch")
    sc = SparkContext(conf=sparkConf)

    conf = {"es.resource": "index/type"}  # assume Elasticsearch is running on localhost defaults
    rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat", \
                             "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                             conf=conf)
    rdd.first()  # the result is a MapWritable that is converted to a Python dict
