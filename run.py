from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from support_func import get_current_date
from properties import MODEL_BUILDING
from _weekly_products import build_prediction_weekly
import properties as p
from _monthly_products import build_prediction_monthly

####################################################################################################################

# Getting Current Date Time for AppName
appName = "_".join([MODEL_BUILDING, "CONSOLIDATED", get_current_date()])
####################################################################################################################

conf = SparkConf().setAppName(appName)

sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

import time


print "Setting LOG LEVEL as ERROR"
sc.setLogLevel("ERROR")

print "Adding jobs.zip to system path"
import sys
sys.path.insert(0, "jobs.zip")

print("Starting Weekly Model building")
start_time = time.time()

for _model_bld_date_string in p._model_bld_date_string:
    print (_model_bld_date_string)
    build_prediction_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)

print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))

# print("Starting Monthly Model building")
# start_time = time.time()
#
# build_prediction_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=p._model_bld_date_string)
# print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))
