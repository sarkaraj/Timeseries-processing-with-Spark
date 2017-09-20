from data_fetch.data_query import get_data
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_arima import _run_dist_arima
from run_distributed_prophet import _run_dist_prophet
from run_distributed_prophet_monthly import _run_dist_prophet_monthly
from support_func import assign_category

conf = SparkConf().setAppName("CONA_TS_MODEL_CREATION_JOB_ID_2").setMaster("yarn-client")
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

print "Add jobs.zip to system path"
import sys

sys.path.insert(0, "jobs.zip")

print "Querying of Hive Table - Obtaining Product Data"
test_data = get_data(sqlContext=sqlContext) \
    .map(lambda x: assign_category(x)) \
    .filter(lambda x: x != "NOT_CONSIDERED")

print "Caching Data"
test_data.cache()

# TESTING PURPOSE ONLY -- TO BE REMOVED
print("TOTAL NUMBER OF INPUT CUSTOMER-PRODUCT Combo (after applying filters) : %s " % test_data.count())
# print test_data.count()

# Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ > 60
print "Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ >= 60"
print "\t--Running distributed arima"
arima_results, count_arima = _run_dist_arima(test_data=test_data, sqlContext=sqlContext)

print "\t--Running distributed prophet"
prophet_results, count_prophet = _run_dist_prophet(test_data=test_data, sqlContext=sqlContext)

# print "Showing ARIMA Results-- \n"
# arima_results.show(2)
# print "Showing PROPHET Results-- \n"
# prophet_results.show(2)

print("Number of CUSTOMER-PRODUCT Combo for ARIMA WEEKLY:: %s" % count_arima)
print("Number of CUSTOMER-PRODUCT Combo for PROPHET WEEKLY:: %s" % count_prophet)

print "\t--Joining the ARIMA + PROPHET Results on same customernumber and matnr"
cond = [arima_results.customernumber == prophet_results.customernumber, arima_results.mat_no == prophet_results.mat_no]
arima_prophet_join_df = arima_results.join(prophet_results, cond).select(arima_results.customernumber,
                                                                         arima_results.mat_no,
                                                                         arima_results.error_arima,
                                                                         arima_results.pred_arima,
                                                                         arima_results.arima_params,
                                                                         prophet_results.error_prophet,
                                                                         prophet_results.pred_prophet,
                                                                         prophet_results.prophet_params,
                                                                         arima_results.pdt_cat)

# arima_prophet_join_df.show(2)

print "Writing the WEEKLY MODEL data into HDFS"
arima_prophet_join_df.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_first_run")

print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))

###################################################################################################
print "**************\n**************\n"

start_time = time.time()

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
print "\t\t--Running distributed prophet"
prophet_monthly_results, count_prophet_monthly = _run_dist_prophet_monthly(test_data=test_data, sqlContext=sqlContext)

print("\nNumber of CUSTOMER-PRODUCT Combo for PROPHET MONTHLY:: %s\n" % count_prophet_monthly)
# print "Showing PROPHET_MONTHLY Results-- \n"
# prophet_monthly_results.show(2)

print "Writing the MONTHLY MODEL data into HDFS"
prophet_monthly_results.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_monthly_first_run")

print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))
