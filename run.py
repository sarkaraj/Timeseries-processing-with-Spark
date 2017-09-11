from data_fetch.data_query import getData
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

from support_func import model_fit

conf = SparkConf().setAppName("test_cona").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)


import time
start_time = time.time()

print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

# print "Adding Extra paths for several site-packages"
# import sys
# sys.path.append('/home/SSHAdmin/.local/lib/python2.7/site-packages/')
# sys.path.append('/home/SSHAdmin/anaconda/lib/python2.7/site-packages/')
# sys.path.insert(0, cona_modules.zip)

print "Addind jobs.zip to system path"
import sys

sys.path.insert(0, "jobs.zip")

print "Querying of Hive Table - Obtaining Product Data"
test_data = getData(sqlContext=sqlContext)

print "test_data number of rows"
print test_data.count()

print "Model_Fit method called"
test_data_rdd = test_data.map(lambda x: model_fit(x))

print "test_data_df create"
test_data_df = test_data_rdd.toDF()
#
# print "test_data_df show"
# test_data_df.show(2)

print "Writing Dataframe to HDFS"
test_data_df.coalesce(2).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/spark_test_1")


print("Time taken for running spark program:\t\t--- %s seconds ---" % (time.time() - start_time))

# print "Attempting to save as textfile"
# test_data_rdd.saveAsTextFile('/tmp/pyspark_data/spark_test_1')
