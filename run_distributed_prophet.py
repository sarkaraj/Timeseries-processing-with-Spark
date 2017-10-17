# from data_fetch.data_query import getData
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import HiveContext
from distributed_grid_search._model_params_set import generate_models_prophet
from transform_data.rdd_to_df import map_for_output_prophet, prophet_output_schema

from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, dist_grid_search_merge_combiner

from distributed_grid_search._fbprophet import run_prophet
from properties import _model_bld_date_string
from transform_data.data_transform import string_to_gregorian

# conf = SparkConf().setAppName("test_cona_distributed_prophet").setMaster("yarn-client")
# sc = SparkContext(conf=conf)
# sqlContext = HiveContext(sparkContext=sc)
#
# print "Running spark jobs distributed PROPHET "
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
#
# # def get_prophet_results(test_data, )
#
# print "Preparing data for parallelizing model grid search"
# test_data_parallel = test_data.flatMap(lambda x: generate_models_prophet(x))
#
# # print test_data_parallel.take(1)
#
# # (customernumber, matnr, data_pd_df_week_aggregated, elem)
# print "Running all models (PROPHET):"
# prophet_results_rdd = test_data_parallel.map(
#     lambda x: run_prophet(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3])).filter(
#     lambda x: x != "MODEL_NOT_VALID")
#
# prophet_results_rdd.cache()
#
# # print prophet_results_rdd.take(2)
# print "prophet_results_rdd.count :: "
# print prophet_results_rdd.count()
#
# # prophet_results_rdd is receiving ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param))
#
# print "Selecting the best prophet models for all customer-product combinations -- running combineByKey"
# opt_prophet_results_rdd = prophet_results_rdd.combineByKey(dist_grid_search_create_combiner, dist_grid_search_merge_value,
#                                                            dist_grid_search_merge_combiner)
# # opt_prophet_results_rdd --> ((cus_no, mat_no),(_criteria, (_criteria, output_error_dict, _prediction, param)))
#
# opt_prophet_results_mapped = opt_prophet_results_rdd.map(lambda line: map_for_output_prophet(line))
#
# # print "printing opt_prophet_results_mapped"
# # print opt_prophet_results_mapped.take(2)
# opt_prophet_results_df = sqlContext.createDataFrame(opt_prophet_results_mapped, schema=prophet_output_schema())
#
# opt_prophet_results_df.printSchema()
#
# opt_prophet_results_df.show()
#
# # print "printing first 2 row of opt_prophet_results_rdd "
# # print opt_prophet_results_rdd.take(2)
#
# # print "Total output records"
# # print opt_prophet_results_rdd.count()
#
#
# print("Time taken for running spark program:\t\t--- %s seconds ---" % (time.time() - start_time))


def _run_dist_prophet(test_data, sqlContext, **kwargs):
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date

    test_data_input = test_data \
        .filter(lambda x: x[1].category in ('I', 'II', 'III'))

    test_data_parallel = test_data_input.flatMap(
        lambda x: generate_models_prophet(x, MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE))

    prophet_results_rdd = test_data_parallel \
        .map(lambda x: run_prophet(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3], min_train_days=x[4].min_train_days,
                                   pdt_cat=x[4].get_product_prop())) \
        .filter(lambda x: x != "MODEL_NOT_VALID") \
        .repartition(60)


    opt_prophet_results_rdd = prophet_results_rdd.combineByKey(dist_grid_search_create_combiner,
                                                               dist_grid_search_merge_value,
                                                               dist_grid_search_merge_combiner,
                                                               numPartitions=20)

    opt_prophet_results_mapped = opt_prophet_results_rdd.map(lambda line: map_for_output_prophet(line))

    opt_prophet_results_df = sqlContext.createDataFrame(opt_prophet_results_mapped, schema=prophet_output_schema())

    return opt_prophet_results_df
