from data_fetch.data_query import getData
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_arima import _run_dist_arima
from run_distributed_prophet import _run_dist_prophet

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

# TESTING PURPOSE ONLY -- TO BE REMOVED
print "test_data number of rows"
print test_data.count()

test_data.cache()

print "Running distributed arima"
arima_results = _run_dist_arima(test_data=test_data, sqlContext=sqlContext)

print "Running distributed prophet"
prophet_results = _run_dist_prophet(test_data=test_data, sqlContext=sqlContext)

arima_results.show()
prophet_results.show()

cond = [arima_results.customernumber == prophet_results.customernumber, arima_results.mat_no == prophet_results.mat_no]
print "joining the two dataframes"
arima_prophet_join_df = arima_results.join(prophet_results, cond).select(arima_results.customernumber,
                                                                         arima_results.mat_no,
                                                                         arima_results.error_arima,
                                                                         arima_results.pred_arima,
                                                                         arima_results.arima_params,
                                                                         prophet_results.error_prophet,
                                                                         prophet_results.pred_prophet,
                                                                         prophet_results.prophet_params)

arima_prophet_join_df.show()

print "Writing the data into HDFS"
arima_prophet_join_df.coalesce(1).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_first_run")

print("Time taken for running spark program:\t\t--- %s seconds ---" % (time.time() - start_time))
