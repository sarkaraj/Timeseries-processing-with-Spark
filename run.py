from data_fetch.data_query import get_data
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_arima import _run_dist_arima
from run_distributed_prophet import _run_dist_prophet
from run_distributed_prophet_monthly import _run_dist_prophet_monthly
from run_moving_average import _run_moving_average_weekly, _run_moving_average_monthly
from support_func import assign_category

conf = SparkConf().setAppName("CONA_TS_MODEL_CREATION_JOB_ID_9")
    # .setMaster("yarn-client")
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


print "Add jobs.zip to system path"
import sys
sys.path.insert(0, "jobs.zip")


# print "Caching Data"
# test_data.cache()

####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################

#############################________________WEEKLY __________#####################################

print "Querying of Hive Table - Obtaining Product Data for Weekly Models"
test_data_weekly_models = get_data(sqlContext=sqlContext, weekly=True) \
    .map(lambda x: assign_category(x)) \
    .filter(lambda x: x != "NOT_CONSIDERED")

#############################________________(ARIMA + PROPHET)__________#####################################



# Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ > 60
print "Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ >= 60"
print "\t--Running distributed arima"
arima_results = _run_dist_arima(test_data=test_data_weekly_models, sqlContext=sqlContext)

print "\t--Running distributed prophet"
prophet_results = _run_dist_prophet(test_data=test_data_weekly_models, sqlContext=sqlContext)

# print "Showing ARIMA Results-- \n"
# arima_results.show(2)
# print "Showing PROPHET Results-- \n"
# prophet_results.show(2)

# print("Number of CUSTOMER-PRODUCT Combo for ARIMA WEEKLY:: %s" % count_arima)
# print("Number of CUSTOMER-PRODUCT Combo for PROPHET WEEKLY:: %s" % count_prophet)

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

print "Writing the WEEKLY_MODELS (ARIMA + PROPHET) data into HDFS"
arima_prophet_join_df.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_first_run")

#############################________________MOVING AVERAGE__________#####################################

print "**************\n**************\n"

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running WEEKLY_MA_MODELS on products\n"
# print "\t\t--Running moving average models"

ma_weekly_results_df = _run_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext)

print "Writing the MA WEEKLY data into HDFS\n"
ma_weekly_results_df.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_ma_weekly")


print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))


####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################

start_time = time.time()

#############################________________MONTHLY__________################################

print "Querying of Hive Table - Obtaining Product Data for Monthly Models"
test_data_monthly_model = get_data(sqlContext=sqlContext, monthly=True) \
    .map(lambda x: assign_category(x)) \
    .filter(lambda x: x != "NOT_CONSIDERED")

#############################________________PROPHET__________################################

print "**************\n**************\n"

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
# print "\t\t--Running distributed prophet"
prophet_monthly_results = _run_dist_prophet_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)


print "Writing the MONTHLY MODEL data into HDFS"
prophet_monthly_results.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_monthly_first_run")

############################________________MOVING AVERAGE__________##########################

print "**************\n**************\n"

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running MONTHLY_MA_MODELS on products\n"
# print "\t\t--Running moving average models"

ma_monthly_results_df = _run_moving_average_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)


print "Writing the MA MONTHLY data into HDFS\n"
ma_monthly_results_df.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_ma_monthly")

print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))
