from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSession, SQLContext
from support_func import get_current_date, get_sample_customer_list, obtain_mdl_bld_dt
from properties import MODEL_BUILDING
from _weekly_products import build_prediction_weekly
import properties as p
from _monthly_products import build_prediction_monthly
import time

####################################################################################################################

# Getting Current Date Time for AppName
appName = "_".join([MODEL_BUILDING, "CONSOLIDATED", get_current_date()])
####################################################################################################################

# conf = SparkConf()

spark = SparkSession \
    .builder \
    .appName(appName) \
    .enableHiveSupport() \
    .getOrCreate()

# sc = SparkContext(conf=conf)
sc = spark.sparkContext
sqlContext = spark


# # # Total Number of executors for the current run
# total_execs = int(sc._jsc.sc().getExecutorMemoryStatus().size())

# # # model building date to be obtained as external argument
# mdl_bld_date_string = obtain_mdl_bld_dt()


print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

print "Adding forecaster.zip to system path"
import sys

sys.path.insert(0, "forecaster.zip")

mdl_bld_date_string = ["".join(sys.argv[1])]
_model_bld_date_string = mdl_bld_date_string[0]

comments = " ".join(["Consolidated Run. Dated:", str(_model_bld_date_string)])

print "Importing Sample Customer List"
get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string, comments=comments)

print("************************************************************************************")
print (_model_bld_date_string)
print("************************************************************************************\n")

print("Starting Weekly Model building")
start_time = time.time()
build_prediction_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)

print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))

if p.monthly_dates.get(_model_bld_date_string) == True:
    print("Starting Monthly Model building")
    start_time = time.time()

    build_prediction_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))

# # Force Stopping SparkContext
spark.stop()
