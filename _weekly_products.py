from run_distributed_arima import _run_dist_arima
# from run_distributed_prophet import _run_dist_prophet
from run_moving_average import _run_moving_average_weekly
from support_func import assign_category, get_current_date
# from transform_data.spark_dataframe_func import final_select_dataset
from properties import MODEL_BUILDING, weekly_pdt_cat_123_location, weekly_pdt_cat_7_location
from pyspark.sql.functions import *
from transform_data.data_transform import string_to_gregorian
from support_func import get_current_date, get_sample_customer_list, raw_data_to_weekly_aggregate, filter_white_noise
import properties as p

def build_prediction_weekly(sc, sqlContext, **kwargs):
    from data_fetch.data_query import get_data_weekly

    if '_model_bld_date_string' in kwargs.keys():
        _model_bld_date_string = kwargs.get('_model_bld_date_string')
    else:
        _model_bld_date_string = p._model_bld_date_string_list

    ####################################################################################################################
    # # Defining the date variables

    week_cutoff_date = _model_bld_date_string  # # is a string
    MODEL_BLD_CURRENT_DATE = string_to_gregorian(week_cutoff_date)  # # is of datetime.date type

    ####################################################################################################################
    ###################################______________WEEKLY_____________################################################
    ####################################################################################################################


    #############################________________DATA_ACQUISITION__________#####################################

    print ("Querying of Hive Table - Obtaining Product Data for Weekly Models")
    test_data_weekly_models = get_data_weekly(sqlContext=sqlContext, week_cutoff_date=week_cutoff_date) \
        .rdd \
        .map(lambda x: assign_category(x)) \
        .filter(lambda x: x != "NOT_CONSIDERED") \
        .map(lambda x: raw_data_to_weekly_aggregate(row_object_cat = x, MODEL_BLD_CURRENT_DATE = MODEL_BLD_CURRENT_DATE))\
        .map(lambda x: filter_white_noise(x))\
        .filter(lambda x: x[3].category in ('I', 'II', 'III', 'VII'))

    # # Caching Data for current run
    test_data_weekly_models.cache()

    #####################################________________ARIMA__________#######################################

    # Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ > 60
    print ("Running WEEKLY_MODELS SARIMAX on products with FREQ >= " + str(p.annual_freq_cut_1))
    print ("\t--Running distributed ARIMA")
    arima_results_to_disk = _run_dist_arima(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                    MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    # print("\t--Temporary writing arima results to disk")
    # arima_results_to_disk \
    #     .write.mode('overwrite') \
    #     .format('orc') \
    #     .option("header", "true") \
    #     .save("/tmp/arima_weekly_temp_write")
    #
    # print("\t--Temporary write complete\n\n")
    #
    # print "\t--Running distributed PROPHET"
    # prophet_results_to_disk = _run_dist_prophet(test_data=test_data_weekly_models, sqlContext=sqlContext,
    #                                     MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)
    #
    # print("\t--Temporary writing arima results to disk")
    # prophet_results_to_disk \
    #     .write.mode('overwrite') \
    #     .format('orc') \
    #     .option("header", "true") \
    #     .save("/tmp/prophet_weekly_temp_write")
    #
    # print("\t--Temporary write complete\n\n")
    #
    # print("ARIMA: Loading temporary written data onto memory\n")
    # arima_results = sqlContext.read.option("header", "true") \
    #     .format('orc') \
    #     .load("/tmp/arima_weekly_temp_write")
    #
    # print("PROPHET: Loading temporary written data onto memory\n")
    # prophet_results = sqlContext.read.option("header", "true") \
    #     .format('orc') \
    #     .load("/tmp/prophet_weekly_temp_write")
    #
    # print "\t--Joining the ARIMA + PROPHET Results on same customernumber and matnr"
    # cond = [arima_results.customernumber_arima == prophet_results.customernumber_prophet,
    #         arima_results.mat_no_arima == prophet_results.mat_no_prophet]
    #
    # prophet_arima_join_df = prophet_results \
    #     .join(arima_results, on=cond, how='outer')
    #
    # prophet_arima_join_df_select_cols = final_select_dataset(prophet_arima_join_df, sqlContext=sqlContext)
    #
    #
    # prophet_arima_join_df_final = prophet_arima_join_df_select_cols \

    arima_results = arima_results_to_disk \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('week_cutoff_date', lit(week_cutoff_date))\
        .withColumn('job_run_date', lit(get_current_date()))

    print ("\t--Writing the WEEKLY_MODELS ARIMA data into HDFS")
    arima_results \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(weekly_pdt_cat_123_location)

    #############################________________MOVING AVERAGE__________#####################################

    print ("\t**************\n**************")

    print ("Running WEEKLY_MODELS MOVING AVERAGE on products  with FREQ >= " + str(p.annual_freq_cut_1))
    print ("\t--Running distributed Moving Average")
    ma_weekly_results_df = _run_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                                      MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    ma_weekly_results_df_final = ma_weekly_results_df \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('week_cutoff_date', lit(week_cutoff_date))\
        .withColumn('job_run_date', lit(get_current_date()))

    print ("\t--Writing the MA WEEKLY data into HDFS\n")
    ma_weekly_results_df_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(weekly_pdt_cat_7_location)

    ####################################################################################################################
    # Clearing cache before the next run
    # sqlContext.clearCache()
    test_data_weekly_models.unpersist()

    print("************************************************************************************")


if __name__ == "__main__":
    # from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext, SparkSession

    ###################################################################################################################
    print ("Add jobs.zip to system path")
    import sys

    sys.path.insert(0, "forecaster.zip")

    ###################################################################################################################

    # from run_distributed_arima import _run_dist_arima
    # from run_distributed_prophet import _run_dist_prophet
    # from run_moving_average import _run_moving_average_weekly
    # from support_func import assign_category, get_current_date
    # from transform_data.spark_dataframe_func import final_select_dataset
    # from properties import MODEL_BUILDING, weekly_pdt_cat_123_location, weekly_pdt_cat_7_location
    # from pyspark.sql.functions import *
    # from transform_data.data_transform import string_to_gregorian
    # from support_func import get_current_date, get_sample_customer_list
    # import properties as p

    ###################################################################################################################

    # Getting Current Date Time for AppName
    appName_Weekly = "_".join([MODEL_BUILDING, "W", get_current_date()])
    ####################################################################################################################

    # conf = SparkConf().setAppName(appName)
    #
    # sc = SparkContext(conf=conf)
    # sqlContext = HiveContext(sparkContext=sc)

    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir",
                "wasb://conahdiv3@conapocv2standardsa.blob.core.windows.net/user/sshuser/spark-warehouse") \
        .appName(appName_Weekly) \
        .enableHiveSupport() \
        .getOrCreate()

    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    sqlContext = spark

    import time

    start_time = time.time()

    print ("Setting LOG LEVEL as ERROR")
    sc.setLogLevel("ERROR")

    mdl_bld_date_string = "".join(sys.argv[1])

    print ("Importing Sample Customer List")
    get_sample_customer_list(sc=sc, sqlContext=sqlContext)

    _model_bld_date_string = mdl_bld_date_string

    print("************************************************************************************")
    print (_model_bld_date_string)
    print("************************************************************************************\n")

    if p.weekly_dates.get(_model_bld_date_string):
        print("Starting Weekly Model building")
        start_time = time.time()

        build_prediction_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))

    # # Clearing cache
    # SQLContext.clearCache()

    # # Force Stopping SparkContext
    # sc.stop()

    spark.stop()
