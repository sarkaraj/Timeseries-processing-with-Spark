from pyspark import SparkConf,SparkContext
from properties import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from data_query import getData
from support_func import model_fit

conf = SparkConf().setAppName("test_cona").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

print "Adding Extra paths for several site-packages"
import sys
sys.path.append('/home/SSHAdmin/.local/lib/python2.7/site-packages/')
sys.path.append('/home/SSHAdmin/anaconda/lib/python2.7/site-packages/')
# sys.path.insert(0, cona_modules.zip)

print "Reading Holidays file"
holidays_data = sc.textFile("/tmp/pyspark_data/holidays")
header = holidays_data.first()
holidays_rdd = holidays_data.filter(lambda x: x != header).map(lambda x: x.split(","))
holiday_list = sqlContext.createDataFrame(holidays_rdd, ['ds', 'holiday', 'lower_window', 'upper_window']).collect()

print "Querying of Hive Table - Obtaining Product Data"
# sc.broadcast(holidays)
test_data = getData(sqlContext=sqlContext)
test_data_rdd = test_data.map(lambda x: model_fit(x, holiday_list))

print "Output - take(1)"
print test_data_rdd.take(1)

test_data_rdd.saveAsTextFile('/tmp/pyspark_data/spark_test_1')




