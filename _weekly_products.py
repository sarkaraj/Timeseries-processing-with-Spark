import pyspark.sql.functions as func
from run_distributed_arima import _run_dist_arima
# from run_distributed_prophet import _run_dist_prophet
from run_moving_average import _run_moving_average_weekly
from support_func import assign_category, get_current_date
# from transform_data.spark_dataframe_func import final_select_dataset
from properties import MODEL_BUILDING, weekly_pdt_cat_123_location, monthly_pdt_cat_456_location, \
    weekly_pdt_cat_7_location, monthly_pdt_cat_8910_location,weekly_flag_location
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transform_data.data_transform import string_to_gregorian
from support_func import get_current_date, get_sample_customer_list, raw_data_to_weekly_aggregate, filter_white_noise, \
    remove_outlier
import properties as p


def build_prediction_weekly(sc, sqlContext, _bottlers, **kwargs):
    # # _bottlers is a broadcast variable
    from data_fetch.data_query import get_data_weekly

    if 'backlog_run' in kwargs.keys() and kwargs.get('backlog_run'):
        backlog = True
    else:
        backlog = False

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

    print("Querying of Hive Table - Obtaining Product Data for Weekly Models")
    test_data_weekly_models = get_data_weekly(sqlContext=sqlContext, week_cutoff_date=week_cutoff_date,
                                              _bottlers=_bottlers) \
        .rdd \
        .map(lambda x: assign_category(x)) \
        .filter(lambda x: x != "NOT_CONSIDERED") \
        .map(lambda x: raw_data_to_weekly_aggregate(row_object_cat=x, MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)) \
        .map(lambda x: remove_outlier(x)) \
        .filter(lambda x: x[3].category in ('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X'))

    # .map(lambda x: filter_white_noise(x)) \

    # # Caching Data for current run
    test_data_weekly_models.cache()

    if backlog:
        print("Backlog Running set to True. Running MA for all categories")
        # ############################________________MOVING AVERAGE__________#####################################

        print("*************************************************************\n")

        print("Running MOVING AVERAGE on products")
        print("\t--Running distributed Moving Average")
        ma_weekly_results_df = _run_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                                          MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE,
                                                          backlog_run=True)

        ma_weekly_results_df_pre_final = ma_weekly_results_df \
            .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
            .withColumn('week_cutoff_date', lit(week_cutoff_date)) \
            .withColumn('load_timestamp', current_timestamp()) \
            .withColumn("category_flag",
                        udf(lambda x: x.get("category"), StringType())(col("pdt_cat")).cast(StringType()))

        print("\t--Writing the MA data into HDFS\n")

        ma_weekly_results_df_pre_final.cache()

        post_outlier_period_flag_df = ma_weekly_results_df_pre_final \
            .select(["customernumber", "mat_no", "mdl_bld_dt", "post_outlier_period_flag", "load_timestamp"])

        post_outlier_period_flag_df \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_flag_location)

        ma_weekly_results_df_final = ma_weekly_results_df_pre_final \
            .drop(col("post_outlier_period_flag"))

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['I', 'II', 'III'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_pdt_cat_123_location)

        print("\t-- 1, 2, 3 -- Completed\n")

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['IV', 'V', 'VI'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(monthly_pdt_cat_456_location)

        print("\t-- 4, 5, 6 -- Completed\n")

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['VII'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_pdt_cat_7_location)

        print("\t-- 7 -- Completed\n")

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['VIII', 'IX', 'X'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(monthly_pdt_cat_8910_location)

        print("\t-- 8, 9, 10 -- Completed\n")

        # ###################################################################################################################
        # Clearing cache before the next run
        # sqlContext.clearCache()

        ma_weekly_results_df_pre_final.unpersist()
        test_data_weekly_models.unpersist()

        print("************************************************************************************")
    else:
        # #####################################________________ARIMA__________#######################################

        # Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ > 60
        print("Running WEEKLY_MODELS SARIMAX on products with FREQ >= " + str(p.annual_freq_cut_1))
        print("*************************************************************\n")
        print("\t--Running distributed ARIMA")
        arima_results_to_disk = _run_dist_arima(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                                MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

        arima_results_pre_final = arima_results_to_disk \
            .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
            .withColumn('week_cutoff_date', lit(week_cutoff_date)) \
            .withColumn('load_timestamp', current_timestamp())

        arima_results_pre_final.cache()

        post_outlier_period_flag_df = arima_results_pre_final \
            .select(
            ["customernumber_arima", "mat_no_arima", "mdl_bld_dt", "post_outlier_period_flag", "load_timestamp"])

        post_outlier_period_flag_df \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_flag_location)

        arima_results = arima_results_pre_final \
            .drop(col("post_outlier_period_flag"))
        
        print("\t--Writing the WEEKLY_MODELS ARIMA data into HDFS")
        arima_results \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_pdt_cat_123_location)

        print("\t-- 1, 2, 3 -- Completed\n")

        arima_results_pre_final.unpersist()

        # ############################________________MOVING AVERAGE__________#####################################

        print("Running MOVING AVERAGE on products")
        print("*************************************************************\n")
        print("\t--Running distributed Moving Average")
        ma_weekly_results_df = _run_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                                          MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

        ma_weekly_results_df_pre_final = ma_weekly_results_df \
            .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
            .withColumn('week_cutoff_date', lit(week_cutoff_date)) \
            .withColumn('load_timestamp', current_timestamp()) \
            .withColumn("category_flag",
                        udf(lambda x: x.get("category"), StringType())(col("pdt_cat")).cast(StringType()))

        print("\t--Writing the MA data into HDFS\n")

        ma_weekly_results_df_pre_final.cache()

        post_outlier_period_flag_df1 = ma_weekly_results_df_pre_final \
            .select(["customernumber", "mat_no", "mdl_bld_dt", "post_outlier_period_flag", "load_timestamp"])

        post_outlier_period_flag_df1 \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_flag_location)

        ma_weekly_results_df_final = ma_weekly_results_df_pre_final.drop(col("post_outlier_period_flag"))

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['IV', 'V', 'VI'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(monthly_pdt_cat_456_location)

        print("\t-- 4, 5, 6 -- Completed\n")

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['VII'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(weekly_pdt_cat_7_location)

        print("\t-- 7 -- Completed\n")

        ma_weekly_results_df_final \
            .filter(col('category_flag').isin(['VIII', 'IX', 'X'])) \
            .drop(col('category_flag')) \
            .coalesce(1) \
            .write.mode(p.WRITE_MODE) \
            .format('orc') \
            .option("header", "false") \
            .save(monthly_pdt_cat_8910_location)

        print("\t-- 8, 9, 10 -- Completed\n")

        ma_weekly_results_df_pre_final.unpersist()

        # ###################################################################################################################
        # Clearing cache before the next run
        # sqlContext.clearCache()
        test_data_weekly_models.unpersist()

        print("************************************************************************************")


if __name__ == "__main__":
    # from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext, SparkSession

    ###################################################################################################################
    print("Add jobs.zip to system path")
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

    print("Setting LOG LEVEL as ERROR")
    sc.setLogLevel("ERROR")

    mdl_bld_date_string = "".join(sys.argv[1])

    print("Importing Sample Customer List")
    get_sample_customer_list(sc=sc, sqlContext=sqlContext)

    _model_bld_date_string = mdl_bld_date_string

    print("************************************************************************************")
    print(_model_bld_date_string)
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
