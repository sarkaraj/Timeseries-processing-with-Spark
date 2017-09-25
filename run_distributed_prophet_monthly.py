# from data_fetch.data_query import getData
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import HiveContext
from distributed_grid_search._model_params_set import generate_models_prophet_monthly
from transform_data.rdd_to_df import map_for_output_prophet, prophet_output_schema

from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, dist_grid_search_merge_combiner

from distributed_grid_search._fbprophet_monthly import run_prophet_monthly



# conf = SparkConf().setAppName("test_cona_distributed_prophet_monthly").setMaster("yarn-client")
# sc = SparkContext(conf=conf)
# sqlContext = HiveContext(sparkContext=sc)
#
# print "Running spark jobs distributed prophet monthly "
#
# import time
#
# start_time = time.time()
#
# print "Setting LOG LEVEL as ERROR"
# sc.setLogLevel("ERROR")
#
# # print "Adding Extra paths for several site-packages"
# # import sys
# # sys.path.append('/home/SSHAdmin/.local/lib/python2.7/site-packages/')
# # sys.path.append('/home/SSHAdmin/anaconda/lib/python2.7/site-packages/')
# # sys.path.insert(0, cona_modules.zip)
#
# print "Addind jobs.zip to system path"
# import sys
#
# sys.path.insert(0, "jobs.zip")
#
# print "Querying of Hive Table - Obtaining Product Data"
# test_data = getData(sqlContext=sqlContext)
#
# # test_data.cache()
#
# # print "test_data number of rows"
# # print test_data.count()
#
# print "Preparing data for parallelizing model grid search"
# test_data_parallel = test_data.flatMap(lambda x: generate_models_prophet_monthly(x))
#
# # print test_data_parallel.take(1)
#
# # (customernumber, matnr, data_pd_df_week_aggregated, elem)
# print "Running all models:"
# prophet_results_rdd = test_data_parallel.map(
#     lambda x: run_prophet_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3])).filter(
#     lambda x: x != "MODEL_NOT_VALID")
#
# prophet_results_rdd.cache()
#
# # print prophet_results_rdd.take(2)
# print "prophet_results_rdd.count :: "
# print prophet_results_rdd.count()
#
# # print prophet_results_rdd.take(2)
#
# # prophet_results_rdd is receiving ((cus_no, mat_no), (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq))
#
# print "Selecting the best arima models for all customer-product combinations -- running combineByKey"
# opt_prophet_results_rdd = prophet_results_rdd.combineByKey(dist_grid_search_create_combiner, dist_grid_search_merge_value,
#                                                            dist_grid_search_merge_combiner)
# # opt_prophet_results_rdd --> ((cus_no, mat_no),(_criteria, (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq)))
#
# # print opt_prophet_results_rdd.take(2)
#
# opt_prophet_results_mapped = opt_prophet_results_rdd.map(lambda line: map_for_output_prophet(line))
#
# opt_prophet_results_df = sqlContext.createDataFrame(opt_prophet_results_mapped, schema=prophet_output_schema())
#
# opt_prophet_results_df.show()
#
# opt_prophet_results_df.coalesce(1).write.mode('overwrite').format('orc').option("header", "false").save(
#     "/tmp/pyspark_data/dist_model_monthly_first_run")
#
#
#
# # print "printing first 2 row of opt_prophet_results_rdd "
# # print opt_prophet_results_rdd.take(2)
#
# # print "Total output records"
# # print opt_prophet_results_rdd.count()
#
#
# print("Time taken for running spark program:\t\t--- %s seconds ---" % (time.time() - start_time))

def _run_dist_prophet_monthly(test_data, sqlContext):
    test_data_input = test_data \
        .filter(lambda x: x[1].category in ['IV', 'V', 'VI'])

    test_data_parallel = test_data_input.flatMap(lambda x: generate_models_prophet_monthly(x))

    prophet_results_rdd = test_data_parallel.map(
        lambda x: run_prophet_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3],
                                      min_train_days=x[4].min_train_days, pdt_cat=x[4].get_product_prop())).filter(
        lambda x: x != "MODEL_NOT_VALID")

    opt_prophet_results_rdd = prophet_results_rdd.combineByKey(dist_grid_search_create_combiner,
                                                               dist_grid_search_merge_value,
                                                               dist_grid_search_merge_combiner)

    opt_prophet_results_mapped = opt_prophet_results_rdd.map(lambda line: map_for_output_prophet(line))

    opt_prophet_results_df = sqlContext.createDataFrame(opt_prophet_results_mapped, schema=prophet_output_schema())

    # return opt_prophet_results_df, opt_prophet_results_df.count()
    return opt_prophet_results_df