from data_fetch.data_query import getData
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

from support_func import model_fit

conf = SparkConf().setAppName("test_cona").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

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

print "Reading Holidays file"
holidays_data = sc.textFile("/tmp/pyspark_data/holidays")
header = holidays_data.first()
holidays_rdd = holidays_data.filter(lambda x: x != header).coalesce(1).map(lambda x: x.split(",")).cache()
holiday_list = sqlContext.createDataFrame(holidays_rdd, ['ds', 'holiday', 'lower_window', 'upper_window']).collect()

print "Querying of Hive Table - Obtaining Product Data"
# sc.broadcast(holidays)
test_data = getData(sqlContext=sqlContext)

print "test_data number of rows"
print test_data.count()

print "Model_Fit method called"
test_data_rdd = test_data.map(lambda x: model_fit(x, holiday_list))

test_data_rdd.cache()

print "Output - take(1)"
print test_data_rdd.take(1)


# print "test_data_df create"
# test_data_df = sqlContext.createDataFrame(test_data_rdd, ['cus_no', 'mat_no', '6wre_med','6wre_max', '6wre_med_prophet',
#                                          '6wre_max_prophet','6wre_med_arima','6wre_max_arima',
#                                          '12wre_med','12wre_max', '12wre_med_prophet','12wre_max_prophet',
#                                          '12wre_med_arima', '12wre_max_arima',
#                                          'cum_error', 'cum_quantity', 'period_days'])
#
# print "test_data_df show"
# test_data_df.show()


# print "Attempting to save as textfile"
# test_data_rdd.saveAsTextFile('/tmp/pyspark_data/spark_test_1')




