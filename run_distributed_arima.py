# from data_fetch.data_query import getData
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import HiveContext
from distributed_grid_search._model_params_set import generate_models_sarimax
from transform_data.rdd_to_df import map_for_output_arima, arima_output_schema
from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, dist_grid_search_merge_combiner
from distributed_grid_search._sarimax import sarimax


# conf = SparkConf().setAppName("test_cona_distributed_arima").setMaster("yarn-client")
# sc = SparkContext(conf=conf)
# sqlContext = HiveContext(sparkContext=sc)
#
# print "Running spark jobs distributed arima "
#
# import time
#
# start_time = time.time()
#
# print "Setting LOG LEVEL as ERROR"
# sc.setLogLevel("ERROR")
#
# print "Addind jobs.zip to system path"
# import sys
#
# sys.path.insert(0, "jobs.zip")
#
# print "Querying of Hive Table - Obtaining Product Data"
# test_data = getData(sqlContext=sqlContext)


# print "Preparing data for parallelizing model grid search"
# test_data_parallel = test_data.flatMap(lambda x: generate_models_sarimax(x))
#
# # print test_data_parallel.take(1)
#
# # cus_no, mat_no, pdq, seasonal_pdq, prod
# print "Running all models:"
# arima_results_rdd = test_data_parallel.map(
#     lambda x: sarimax(cus_no=x[0], mat_no=x[1], pdq=x[2], seasonal_pdq=x[3], prod=x[4])).filter(
#     lambda x: x != "MODEL_NOT_VALID")
#
# arima_results_rdd.cache()
#
# # print prophet_results_rdd.take(2)
# print "prophet_results_rdd.count :: "
# print arima_results_rdd.count()
#
# # prophet_results_rdd is receiving ((cus_no, mat_no), (_criteria, output_error_dict, _output_pred, list(pdq), list(seasonal_pdq)))
#
# print "Selecting the best arima models for all customer-product combinations -- running combineByKey"
# opt_arima_results_rdd = arima_results_rdd.combineByKey(dist_grid_search_create_combiner, dist_grid_search_merge_value,
#                                                        dist_grid_search_merge_combiner)
#
# opt_arima_results_mapped = opt_arima_results_rdd.map(lambda line : map_for_output_arima(line))
#
# opt_arima_results_df = sqlContext.createDataFrame(opt_arima_results_mapped, schema=arima_output_schema())
#
# opt_arima_results_df.printSchema()
#
# opt_arima_results_df.show()
#
# # print "printing first 2 row of opt_arima_results_rdd "
# # print opt_arima_results_rdd.take(2)
# #
# # # print "Total output records"
# # # print opt_arima_results_rdd.count()
#
#
# print("Time taken for running spark program:\t\t--- %s seconds ---" % (time.time() - start_time))


def _run_dist_arima(test_data, sqlContext):
    # from pyspark.sql.functions import *
    # from pyspark.sql.types import *

    test_data_input = test_data \
        .filter(lambda x: x[1].category in ['I', 'II', 'III'])

    test_data_parallel = test_data_input.flatMap(lambda x: generate_models_sarimax(x))

    # print test_data_parallel.take(1)

    # cus_no, mat_no, pdq, seasonal_pdq, prod
    # print "Running all models:"
    arima_results_rdd = test_data_parallel \
        .map(lambda x: sarimax(cus_no=x[0], mat_no=x[1], pdq=x[2], seasonal_pdq=x[3], prod=x[4],
                               min_train_days=x[5].min_train_days, pdt_cat=x[5].get_product_prop())) \
        .filter(lambda x: x != "MODEL_NOT_VALID")

    # prophet_results_rdd is receiving ((cus_no, mat_no), (_criteria, output_error_dict, _output_pred, list(pdq), list(seasonal_pdq), pdt_cat))

    opt_arima_results_rdd = arima_results_rdd.combineByKey(dist_grid_search_create_combiner,
                                                           dist_grid_search_merge_value,
                                                           dist_grid_search_merge_combiner)

    opt_arima_results_mapped = opt_arima_results_rdd.map(lambda line: map_for_output_arima(line))

    opt_arima_results_df = sqlContext.createDataFrame(opt_arima_results_mapped, schema=arima_output_schema())

    return opt_arima_results_df, opt_arima_results_df.count()
