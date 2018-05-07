# from pyspark.sql.types import *
#
# def _which_prophet_module():
#     import fbprophet
#     import pystan
#     _result = str(fbprophet.__file__), str(pystan.__file__)
#     return _result
#
#
# def _schema():
#     proph = StructField("fb_loc", StringType())
#     pystan = StructField("pystan_loc", StringType())
#     _result = StructType([proph, pystan])
#     return _result


def _run_dist_prophet_monthly(test_data, sqlContext, **kwargs):
    # LIBRARY IMPORTS
    from distributed_grid_search._model_params_set import generate_models_prophet_monthly
    from transform_data.rdd_to_df import map_for_output_prophet, prophet_output_schema

    from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, \
        dist_grid_search_merge_combiner

    from distributed_grid_search._fbprophet_monthly import run_prophet_monthly
    from properties import REPARTITION_STAGE_1, REPARTITION_STAGE_2

    # ###################

    test_data_input = test_data \
        .filter(lambda x: x[1].category in ('IV', 'V', 'VI'))

    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')

    test_data_parallel = test_data_input.flatMap(
        lambda x: generate_models_prophet_monthly(x,
                                                  MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE))  # # gets 587 * 55 = 32285 rows

    # # Parallelizing Jobs
    prophet_results_rdd = test_data_parallel \
        .repartition(REPARTITION_STAGE_1) \
        .map(lambda x: run_prophet_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3],
                                           min_train_days=x[4].min_train_days, pdt_cat=x[4].get_product_prop())) \
        .filter(lambda x: x != "MODEL_NOT_VALID")
    # .repartition(REPARTITION_STAGE_1)

    opt_prophet_results_rdd = prophet_results_rdd \
        .combineByKey(dist_grid_search_create_combiner,
                      dist_grid_search_merge_value,
                      dist_grid_search_merge_combiner,
                      numPartitions=REPARTITION_STAGE_2)

    opt_prophet_results_mapped = opt_prophet_results_rdd.map(lambda line: map_for_output_prophet(line))

    opt_prophet_results_df = sqlContext.createDataFrame(opt_prophet_results_mapped, schema=prophet_output_schema())

    return opt_prophet_results_df


# FOR MODULE TESTING

# if __name__ == "__main__":
#     from data_fetch.data_query import get_data_weekly, get_data_monthly
#     from pyspark import SparkContext, SparkConf
#     from pyspark.sql import HiveContext
#     # from run_distributed_arima import _run_dist_arima
#     # from run_distributed_prophet import _run_dist_prophet
#     # from run_distributed_prophet_monthly import _run_dist_prophet_monthly
#     # from run_moving_average import _run_moving_average_weekly, _run_moving_average_monthly
#     from support_func import assign_category
#
#     # from transform_data.spark_dataframe_func import final_select_dataset
#
#     conf = SparkConf().setAppName("CONA_TS_MODEL_VALIDATION_JOB_ID_15")
#     # .setMaster("yarn-client")
#     sc = SparkContext(conf=conf)
#     sqlContext = HiveContext(sparkContext=sc)
#
#     import time
#
#     start_time = time.time()
#
#     print "Setting LOG LEVEL as ERROR"
#     sc.setLogLevel("ERROR")
#
#     # print "Adding Extra paths for several site-packages"
#     # import sys
#     # sys.path.append('/home/SSHAdmin/.local/lib/python2.7/site-packages/')
#     # sys.path.append('/home/SSHAdmin/anaconda/lib/python2.7/site-packages/')
#
#
#     print "Add jobs.zip to system path"
#     import sys
#
#     sys.path.insert(0, "jobs.zip")
#
#     print "Querying of Hive Table - Obtaining Product Data for Monthly Models"
#     test_data_monthly_model = get_data_monthly(sqlContext=sqlContext) \
#         .map(lambda x: assign_category(x)) \
#         .filter(lambda x: x != "NOT_CONSIDERED")
#
#     #############################________________PROPHET__________################################
#
#     print "**************\n**************\n"
#
#     # Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
#     print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
#     # print "\t\t--Running distributed prophet"
#     prophet_monthly_results = _run_dist_prophet_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)
#
#     prophet_monthly_results.distinct().show()
#
#     # # print prophet_monthly_results
#     #
#     # print "Writing the MONTHLY MODEL data into HDFS"
#     # prophet_monthly_results.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
#     #     "/tmp/pyspark_data/dist_model_monthly_first_run_testing")
