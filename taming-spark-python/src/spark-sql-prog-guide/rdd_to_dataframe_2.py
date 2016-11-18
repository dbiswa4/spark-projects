from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *

'''
Ref:
https://spark.apache.org/docs/1.6.1/sql-programming-guide.html
#Programmatically Specifying the Schema

#Solution for 'Unresolved StructField' issue?
https://github.com/databricks/spark-csv

A good stackoverflow link,
http://stackoverflow.com/questions/36026070/building-a-structtype-from-a-dataframe-in-pyspark

Note:
[2016-09-03]
=> Modified for Spark 2.0
'''

def display_people(people):
    print 'Control is in display_people(people) method'
    print people
    print 'Name : ' + people[0] + ' and age : ' + people[1]
    print 'Name : ' + people['name'] + ' and age : ' + people['age']


def programmatically_specify_schema(sparkSession):
    '''
    1. Create an RDD of Rows from the original RDD;
    2. Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    3. Apply the schema to the RDD of Rows via createDataFrame method provided by SQLContext.
    '''

    lines = sparkSession.sparkContext.textFile("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p:(p[0], p[1].strip()))

    print 'sc.textFile() returns lines, type is : ', type(lines)
    print 'lines.map() return type is : ', type(parts)
    print 'parts.map() return type is : ', type(parts)


    #Inferring the Schema Using Reflection concept
    #Give fields a name
    #people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    # Infer the schema, and register the DataFrame as a table.
    #schemaPeople = sqlContext.createDataFrame(people)
    #schemaPeople.registerTempTable("people")


    #Programmatically Specifying the Schema concept
    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    print 'StructType returns : ', type(StructType)

    # Apply the schema to the RDD to create a DataFrame
    schemaPeople = sparkSession.createDataFrame(people, schema)

    print 'schemaPeople returns : ', type(schemaPeople)

    '''
    'lines' is of pyspark.rdd.RDD type
    'parts' and 'people' are of pyspark.rdd.PipelinedRDD type
    'schemaPeople' is of pyspark.sql.dataframe.DataFrame type
    select method of DataFrame returns pyspark.sql.dataframe.DataFrame

    The example here shows how we attach a schema to an RDD and converts it into DataFrame

    Demo of different DataFrame methods
    '''
    # Show the content of the DataFrame
    schemaPeople.show()
    # Print the schema in a tree format
    schemaPeople.printSchema()
    schemaPeople.select(schemaPeople['name'], schemaPeople['age']).show()
    schemaPeople.filter(schemaPeople['age'] > 30).show()
    schemaPeople.groupBy('age').count().show()

    print 'schemaPeople.select(schemaPeople["name"], schemaPeople["age"]) type : ', type(schemaPeople.select(schemaPeople['name'], schemaPeople['age']))

    # The results of SQL queries are RDDs and support all the normal RDD operations.
    records = schemaPeople.map(lambda x : x.split() )

    '''
    peopleAbove30 is a DataFrame
    type of peopleAbove30.collect() :  <type 'list'>
    type of people :  <class 'pyspark.sql.types.Row'>
    Name : Optimus Prime and age : 41
    '''
    peopleAbove30 = schemaPeople.filter(schemaPeople['age'] > 30)

    for people in peopleAbove30.collect():
        print 'type of peopleAbove30.collect() : ', type(peopleAbove30.collect())
        print 'type of people : ', type(people)
        print 'Name : ' + people['name'] + ' and age : ' + people['age']



    #Let's iterate in a different way
    '''
    type of peoples :  <class 'pyspark.rdd.RDD'>
    type of peoples.collect() :  <type 'list'>
    type of people :  <class 'pyspark.sql.types.Row'>

    Row(name=u'Iron Man', age=u'32')
    Name : Iron Man and age : 32
    '''
    #Calling map() results in failure
    #peoples = peopleAbove30.map(lambda x: x.split())
    peoples = peopleAbove30.rdd;

    print '\ntype of peoples : ', type(peoples)
    print 'type of peoples.collect() : ', type(peoples.collect())

    for people in peoples.collect():
        print 'type of people : ', type(people)
        print people
        print 'Name : ' + people[0] + ' and age : ' + people[1]
        #Interesting
        #Even after converting to rdd, schema is retained
        #Is it because it's a Row object?
        print 'Name : ' + people['name'] + ' and age : ' + people['age']



    #Let's try to make map() method work on peopleAbove30 DataFrame
    print "\n\nLet's try to make map() method work on peopleAbove30 DataFrame"
    peopleAbove30.map(display_people).collect()

    '''
    => Saving output to file
    Note:
    => In this example, a directory named 'people.json' will be created. Inside the
    directory, part files will be created which will be in json format
    '''
    schemaPeople.select(schemaPeople['name'], schemaPeople['age']).write.save("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/people.json", format="json")



if __name__ == '__main__':

    '''
    #sparkConf = SparkConf().setMaster("local").setAppName("Programmatically Specifying the Schema")
    sparkConf = SparkConf().setAppName("Programmatically Specifying the Schema")
    sc = SparkContext(conf=sparkConf)
    sqlContext = SQLContext(sc)

    programmatically_specify_schema(sqlContext)
    '''
    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:/Users/dbiswas/Documents/Software/Spark/spark-workspace").appName("SparkSQL").getOrCreate()

    programmatically_specify_schema(spark)

    #It gives following error
    #SQLContext' object has no attribute 'stop'
    #sqlContext.stop()


