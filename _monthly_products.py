from data_fetch.data_query import get_data_monthly
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_prophet_monthly import _run_dist_prophet_monthly
from run_moving_average import _run_moving_average_monthly
from support_func import assign_category, get_current_date
from properties import MODEL_BUILDING, monthly_pdt_cat_456_location, monthly_pdt_cat_8910_location
from pyspark.sql.functions import current_date

###################################################################################################################

# Getting Current Date Time for AppName
appName = "_".join([MODEL_BUILDING, "M", get_current_date()])
####################################################################################################################

conf = SparkConf().setAppName(appName)

sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

import time

start_time = time.time()

print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

print "Add jobs.zip to system path"
import sys

sys.path.insert(0, "jobs.zip")

####################################################################################################################
###################################______________MONTHLY_____________###############################################
####################################################################################################################

print "Querying of Hive Table - Obtaining Product Data for Monthly Models"
test_data_monthly_model = get_data_monthly(sqlContext=sqlContext) \
    .map(lambda x: assign_category(x)) \
    .filter(lambda x: x != "NOT_CONSIDERED") \
    .filter(lambda x: x[1].category in ('IV', 'V', 'VI', 'VIII', 'IX', 'X'))

test_data_monthly_model.cache()

#############################________________PROPHET__________################################

print "**************\n**************\n"

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
# print "\t\t--Running distributed prophet"
prophet_monthly_results = _run_dist_prophet_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)

prophet_monthly_results_final = prophet_monthly_results \
    .withColumn('mdl_bld_dt', current_date())

# print prophet_monthly_results

print "Writing the MONTHLY MODEL data into HDFS"
prophet_monthly_results_final \
    .coalesce(4) \
    .write.mode('append') \
    .format('orc') \
    .option("header", "false") \
    .save(monthly_pdt_cat_456_location)

############################________________MOVING AVERAGE__________##########################

print "**************\n**************\n"

# Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
print "Running MONTHLY_MA_MODELS on products\n"
# print "\t\t--Running moving average models"

ma_monthly_results_df = _run_moving_average_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)

ma_monthly_results_df_final = ma_monthly_results_df \
    .withColumn('mdl_bld_dt', current_date())

print "Writing the MA MONTHLY data into HDFS\n"
ma_monthly_results_df_final \
    .coalesce(4) \
    .write.mode('append') \
    .format('orc') \
    .option("header", "false") \
    .save(monthly_pdt_cat_8910_location)

print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))
