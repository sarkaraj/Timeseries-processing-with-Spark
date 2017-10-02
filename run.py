from data_fetch.data_query import get_data_weekly, get_data_monthly
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_arima import _run_dist_arima
from run_distributed_prophet import _run_dist_prophet
from run_distributed_prophet_monthly import _run_dist_prophet_monthly
from run_moving_average import _run_moving_average_weekly, _run_moving_average_monthly
from support_func import assign_category
from transform_data.spark_dataframe_func import final_select_dataset

conf = SparkConf().setAppName("CONA_TS_MODEL_VALIDATION_JOB_ID_15")
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

# ####################################################################################################################
# ####################################################################################################################
# ####################################################################################################################
# ####################################################################################################################
#
# #############################________________WEEKLY __________#####################################
#
# print "Querying of Hive Table - Obtaining Product Data for Weekly Models"
# test_data_weekly_models = get_data_weekly(sqlContext=sqlContext) \
#     .map(lambda x: assign_category(x)) \
#     .filter(lambda x: x != "NOT_CONSIDERED")
#
# #############################________________(ARIMA + PROPHET)__________#####################################
#
#
#
# # Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ > 60
# print "Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ >= 60"
# print "\t--Running distributed arima"
# arima_results = _run_dist_arima(test_data=test_data_weekly_models, sqlContext=sqlContext)
#
# print "\t--Running distributed prophet"
# prophet_results = _run_dist_prophet(test_data=test_data_weekly_models, sqlContext=sqlContext)
#
# # print "Showing ARIMA Results-- \n"
# # arima_results.show(2)
# # print "Showing PROPHET Results-- \n"
# # prophet_results.show(2)
#
# # print("Number of CUSTOMER-PRODUCT Combo for ARIMA WEEKLY:: %s" % count_arima)
# # print("Number of CUSTOMER-PRODUCT Combo for PROPHET WEEKLY:: %s" % count_prophet)
#
# print "\t--Joining the ARIMA + PROPHET Results on same customernumber and matnr"
# cond = [arima_results.customernumber_arima == prophet_results.customernumber_prophet, arima_results.mat_no_arima == prophet_results.mat_no_prophet]
# prophet_arima_join_df = prophet_results\
#     .join(arima_results, on=cond, how='outer')
#
# prophet_arima_join_df_final = final_select_dataset(prophet_arima_join_df, sqlContext=sqlContext)
#
# # arima_prophet_join_df.show(2)
#
# print "Writing the WEEKLY_MODELS (ARIMA + PROPHET) data into HDFS"
# prophet_arima_join_df_final.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
#     "/tmp/pyspark_data/dist_model_first_run")
#
# #############################________________MOVING AVERAGE__________#####################################
#
# print "**************\n**************\n"
#
# # Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
# print "Running WEEKLY_MA_MODELS on products\n"
# # print "\t\t--Running moving average models"
#
# ma_weekly_results_df = _run_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext)
#
# print "Writing the MA WEEKLY data into HDFS\n"
# ma_weekly_results_df.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
#     "/tmp/pyspark_data/dist_model_ma_weekly")
#
#
# print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))


####################################################################################################################
####################################################################################################################
####################################################################################################################
####################################################################################################################

# start_time = time.time()

#############################________________MONTHLY__________################################

print "Querying of Hive Table - Obtaining Product Data for Monthly Models"
test_data_monthly_model = get_data_monthly(sqlContext=sqlContext) \
    .map(lambda x: assign_category(x)) \
    .filter(lambda x: x != "NOT_CONSIDERED")

#############################________________PROPHET__________################################

print "**************\n**************\n"

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
# print "\t\t--Running distributed prophet"
prophet_monthly_results = _run_dist_prophet_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)

# print prophet_monthly_results

print "Writing the MONTHLY MODEL data into HDFS"
prophet_monthly_results.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    "/tmp/pyspark_data/dist_model_monthly_first_run_testing")

# ############################________________MOVING AVERAGE__________##########################
#
# print "**************\n**************\n"
#
# # Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
# print "Running MONTHLY_MA_MODELS on products\n"
# # print "\t\t--Running moving average models"
#
# ma_monthly_results_df = _run_moving_average_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)
#
#
# print "Writing the MA MONTHLY data into HDFS\n"
# ma_monthly_results_df.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
#     "/tmp/pyspark_data/dist_model_ma_monthly")

print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))
