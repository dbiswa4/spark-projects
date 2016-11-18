from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.mllib.stat import Statistics
import csv

'''
Note:
https://www.mapr.com/ebooks/spark/05-apache-spark-use-case-user-profiles.html
'''

def make_tracks_kv(str):
    l = str.split(",")
    '''
    Note:
    Pay attention to [] used as opposed to ().
    [x,[[y, z]]]
    Apprently one extra pair of [] has been used around [y, z]. To my understanding,
    it is to tell Spark that there will be Array of [y, z] which itself is an Array.
    That means Array of Array.
    If we do not use additional pair of [], reduceByKey() operation just creates one
    single comma separated string out of all possible values of [y, z].
    We get similar effect if we use () braces
    '''
    return [l[1], [[int(l[2]), l[3], int(l[4]), l[5]]]]

def make_tracks_kv_other(str):
    l = str.split(",")
    '''
    Does produce a big long string of all the values for a key when used reduceByKey()
    '''
    return (l[1], ((int(l[2]), l[3], int(l[4]), l[5])))


'''
=> Average number of tracks during each period of the day (time ranges are arbitrarily defined in the code)
=> Total unique tracks, i.e., the set of unique track IDs
=> Total mobile tracks, i.e., tracks played when the mobile flag was set
'''
def compute_stats_byuser(tracks):
    mcount = morn = aft = eve = night = 0
    tracklist = []
    for t in tracks:
        trackid, dtime, mobile, zip = t

        #Keeing track of unique tracks customer listened
        if trackid not in tracklist:
            tracklist.append(trackid)

        d, t = dtime.split(" ")
        hourofday = int(t.split(":")[0])
        mcount += mobile

        if(hourofday < 5):
            night += 1
        elif (hourofday < 12):
            morn += 1
        elif (hourofday < 17):
            aft += 1
        elif (hourofday < 22):
            eve += 1
        else:
            night += 1

    return [len(tracklist), morn, aft, eve, night, mcount]



if __name__ == '__main__':
    #sparkConf = SparkConf().setMaster("local[*]").setAppName("Music Profile")
    sparkConf = SparkConf().setAppName("Music Profile")
    sc = SparkContext(conf=sparkConf)

    trackfile = sc.textFile('/Users/dbiswas/Documents/misc/public_datasets/music/tracks.csv')

    #make a k, v RDD out of the input data
    tbycust = trackfile.map(lambda line: make_tracks_kv(line))
    tbycust_kv = tbycust.reduceByKey(lambda a, b: a + b)

    '''
    type of tbycust <class 'pyspark.rdd.PipelinedRDD'>
    tbycust :  [u'48', [[453, u'"2014-10-23 03:26:20"', 0, u'"72132"']]]
    type of tbycust_kv :  <class 'pyspark.rdd.PipelinedRDD'>
    (u'4446', [[511, u'"2014-12-04 03:35:27"', 1, u'"23081"'], [527, u'"2014-11-26 21:31:06"', 0, u'"96142"']])
    '''

    print 'type of tbycust', type(tbycust)
    print 'tbycust : ', tbycust.first()
    print 'type of tbycust_kv : ', type(tbycust_kv)
    print 'tbycust_kv : ', tbycust_kv.first()

    '''
    Experiment with () braces
    '''
    #make a k, v RDD out of the input data
    #tbycust = trackfile.map(lambda line: make_tracks_kv_other(line))
    #tbycust_kv = tbycust.reduceByKey(lambda a, b: a + b)

    '''
    ***
    type of tbycust <class 'pyspark.rdd.PipelinedRDD'>
    tbycust :  (u'48', (453, u'"2014-10-23 03:26:20"', 0, u'"72132"'))
    type of tbycust_kv :  <class 'pyspark.rdd.PipelinedRDD'>
    (u'4446', (511, u'"2014-12-04 03:35:27"', 1, u'"23081"', 527, u'"2014-11-26 21:31:06"', 0, u'"96142"'))
    '''

    #print '\n***\ntype of tbycust', type(tbycust)
    #print 'tbycust : ', tbycust.first()
    #print 'type of tbycust_kv : ', type(tbycust_kv)
    #print 'tbycust_kv : ', tbycust_kv.first()

    #compute profile for each user
    custdata = tbycust_kv.mapValues(lambda a: compute_stats_byuser(a))

    '''
    custdata :  (u'4446', [103, 26, 25, 26, 30, 53])
    type of custdata :  <type 'tuple'>
    '''
    print 'custdata : ', custdata.first()
    print 'type of custdata : ', type(custdata.first())

    count = 0

    for profile in custdata.collect():

        '''
        profile[0] :  3724
        type of profile[0] <type 'unicode'>
        profile[1] :  [102, 29, 24, 26, 34, 60]
        type of profile[1] <type 'list'>
        profile[1][0] :  102
        profile[1][1] :  29
        '''
        print 'profile[0] : ', profile[0]
        print 'type of profile[0]', type(profile[0])
        print 'profile[1] : ', profile[1]
        print 'type of profile[1]', type(profile[1])
        print 'profile[1][0] : ', profile[1][0]
        print 'profile[1][1] : ', profile[1][1]
        count += 1
        if count > 2:
            break

    # compute aggregate stats for entire track history
    aggdata = Statistics.colStats(custdata.map(lambda x: x[1]))

    '''
    type of aggdata :  <class 'pyspark.mllib.stat._statistics.MultivariateStatisticalSummary'>
    '''
    print 'type of aggdata : ', type(aggdata)

    #custdata.saveAsTextFile("/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/live_table.txt")

    for k, v in custdata.collect():
        unique, morn, aft, eve, night, mobile = v
        tot = morn + aft + eve + night

        # persist the data, in this case write to a file
        with open('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/live_table.csv', 'a') as csvfile:
            fwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            fwriter.writerow((k, tot, unique, morn, aft, eve, night, mobile))

        # do the same with the summary data
        with open('/Users/dbiswas/Documents/SourceCode/Spark/spark-projects/taming-spark-python/output/agg_table.csv', 'a') as csvfile:
            fwriter = csv.writer(csvfile, delimiter=' ',
                                 quotechar='|', quoting=csv.QUOTE_MINIMAL)
            fwriter.writerow(Row(aggdata.mean()[0], aggdata.mean()[1],
                             aggdata.mean()[2], aggdata.mean()[3], aggdata.mean()[4],
                             aggdata.mean()[5]))




