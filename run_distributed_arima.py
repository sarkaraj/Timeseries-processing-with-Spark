from data_fetch.data_query import getData
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from distributed_grid_search._model_params_set import generate_models

from support_func import model_fit

from distributed_grid_search._sarimax import sarimax

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

print "Preparing data for parallelizing model grid search"
test_data_parallel = test_data.flatMap(lambda x: generate_models(x))

print "Running all models:"
arima_results_rdd = test_data_parallel.map(lambda x: sarimax(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3]))

# TODO: Yet to be decided what exactly arima_results_rdd is receiving

print "Selecting the best arima models for all customer-product combinations"
# TODO: use combineByKey() here since then per node load get decreased during reduction phase. Logic will be dependent on some model selection criteria


