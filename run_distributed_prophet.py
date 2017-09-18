from data_fetch.data_query import getData
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from distributed_grid_search._model_params_set import generate_models_prophet

from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, dist_grid_search_merge_combiner

from distributed_grid_search._fbprophet import run_prophet

conf = SparkConf().setAppName("test_cona_distributed_prophet").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sparkContext=sc)

print "Running spark jobs Debugging mode "

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

# test_data.cache()

# print "test_data number of rows"
# print test_data.count()

print "Preparing data for parallelizing model grid search"
test_data_parallel = test_data.flatMap(lambda x: generate_models_prophet(x))

# print test_data_parallel.take(1)

# (customernumber, matnr, data_pd_df_week_aggregated, elem)
print "Running all models:"
arima_results_rdd = test_data_parallel.map(
    lambda x: run_prophet(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3])).filter(
    lambda x: x != "MODEL_NOT_VALID")

arima_results_rdd.cache()

print arima_results_rdd.take(2)
print "prophet_results_rdd.count :: "
print arima_results_rdd.count()
# arima_results_rdd is receiving ((cus_no, mat_no), (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq))

# print "Selecting the best arima models for all customer-product combinations -- running combineByKey"
# opt_arima_results_rdd = arima_results_rdd.combineByKey(dist_grid_search_create_combiner, dist_grid_search_merge_value,
#                                                        dist_grid_search_merge_combiner)
# # opt_arima_results_rdd --> ((cus_no, mat_no),(_criteria, (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq)))
#
# # opt_arima_results_rdd.cache()
#
# print "printing first 5 row of opt_arima_results_rdd "
# print opt_arima_results_rdd.take(2)
#
# print "Total output records"
# print opt_arima_results_rdd.count()


print("Time taken for running spark program:\t\t--- %s seconds ---" % (time.time() - start_time))