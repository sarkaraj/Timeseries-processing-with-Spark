# from data_fetch.data_query import get_data_monthly
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import HiveContext
# from run_distributed_prophet_monthly import _run_dist_prophet_monthly
from run_distributed_arima_monthly import _run_dist_arima_monthly
from pyspark.sql.functions import *
from run_moving_average import _run_moving_average_monthly
from support_func import assign_category, get_current_date, _get_last_day_of_previous_month
from properties import MODEL_BUILDING, monthly_pdt_cat_456_location, monthly_pdt_cat_8910_location
from pyspark.sql.functions import *
from transform_data.data_transform import string_to_gregorian
from support_func import get_current_date, get_sample_customer_list, raw_data_to_monthly_aggregate, filter_white_noise
import properties as p


def build_prediction_monthly(sc, sqlContext, **kwargs):
    from data_fetch.data_query import get_data_monthly

    if '_model_bld_date_string' in kwargs.keys():
        _model_bld_date_string = kwargs.get('_model_bld_date_string')
    else:
        _model_bld_date_string = p._model_bld_date_string_list

    ####################################################################################################################
    # # Defining the date variables

    month_cutoff_date = _get_last_day_of_previous_month(string_to_gregorian(
        _model_bld_date_string))  # # returns the last day of previous month for given "_model_bld_date_string_list"

    MODEL_BLD_CURRENT_DATE = string_to_gregorian(month_cutoff_date)  # # is of datetime.date type

    ####################################################################################################################
    ###################################______________MONTHLY_____________###############################################
    ####################################################################################################################

    print ("Querying of Hive Table - Obtaining Product Data for Monthly Models")
    test_data_monthly_model = get_data_monthly(sqlContext=sqlContext, month_cutoff_date=month_cutoff_date) \
        .rdd \
        .map(lambda x: assign_category(x)) \
        .filter(lambda x: x != "NOT_CONSIDERED") \
        .map(lambda x: raw_data_to_monthly_aggregate(row_object_cat= x, MODEL_BLD_CURRENT_DATE= MODEL_BLD_CURRENT_DATE))\
        .map(lambda x: filter_white_noise(x))\
        .filter(lambda x: x[3].category in ('IV', 'V', 'VI', 'VIII', 'IX', 'X'))

    # # Caching Data for this run
    test_data_monthly_model.cache()

    # print("Printing test_data_monthly_model")
    # print(test_data_monthly_model.take(10))

    # #############################________________PROPHET__________################################
    #
    # # Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
    # print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
    # # print "\t\t--Running distributed prophet"
    # prophet_monthly_results = _run_dist_prophet_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext,
    #                                                     MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)
    #
    # prophet_monthly_results_final = prophet_monthly_results \
    #     .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
    #     .withColumn('month_cutoff_date', lit(month_cutoff_date))
    #
    # print "Writing the MONTHLY MODEL data into HDFS"
    # prophet_monthly_results_final \
    #     .write.mode('append') \
    #     .format('orc') \
    #     .option("header", "false") \
    #     .save(monthly_pdt_cat_456_location)

    #############################________________SARIMAX__________################################

    # Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60

    print (
            "Running MONTHLY_MODELS SARIMAX on products with FREQ : " + str(p.annual_freq_cut_2) + " <= X < " + str(
        p.annual_freq_cut_1) + "\n")

    # print "\t\t--Running distributed prophet"
    arima_monthly_results = _run_dist_arima_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext,
                                                    MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    arima_monthly_results_final = arima_monthly_results \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('month_cutoff_date', lit(month_cutoff_date))\
        .withColumn('job_run_date', lit(get_current_date()))

    # print("Printing arima_monthly_results_final")
    # arima_monthly_results_final.show(10)

    print ("Writing the MONTHLY MODEL data into HDFS")
    arima_monthly_results_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(monthly_pdt_cat_456_location)

    ############################________________MOVING AVERAGE__________##########################

    print ("**************\n**************\n")

    print ("Running MONTHLY_MODELS MOVING AVERAGE FOR CATEGORY VIII, IX, X")

    ma_monthly_results_df = _run_moving_average_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext,
                                                        MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    ma_monthly_results_df_final = ma_monthly_results_df \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('month_cutoff_date', lit(month_cutoff_date))\
        .withColumn('job_run_date', lit(get_current_date()))

    print "Writing the MA MONTHLY data into HDFS\n"
    ma_monthly_results_df_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(monthly_pdt_cat_8910_location)

    # # Clearing cache
    # sqlContext.clearCache()

    test_data_monthly_model.unpersist()

    print("************************************************************************************")


if __name__ == "__main__":
    # from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext, SparkSession, SQLContext

    ###################################################################################################################
    print "Add jobs.zip to system path"
    import sys

    sys.path.insert(0, "forecaster.zip")

    ###################################################################################################################

    # from run_distributed_prophet_monthly import _run_dist_prophet_monthly
    # from run_moving_average import _run_moving_average_monthly
    # from support_func import assign_category, get_current_date, _get_last_day_of_previous_month
    # from properties import MODEL_BUILDING, monthly_pdt_cat_456_location, monthly_pdt_cat_8910_location
    # from pyspark.sql.functions import *
    # from transform_data.data_transform import string_to_gregorian
    # from support_func import get_current_date, get_sample_customer_list
    # import properties as p

    ###################################################################################################################

    # Getting Current Date Time for AppName
    appName_Monthly = "_".join([MODEL_BUILDING, "M", get_current_date()])
    ####################################################################################################################

    # conf = SparkConf().setAppName(appName)
    #
    # sc = SparkContext(conf=conf)
    # sqlContext = HiveContext(sparkContext=sc)

    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir",
                "wasb://conahdiv3@conapocv2standardsa.blob.core.windows.net/user/sshuser/spark-warehouse") \
        .appName(appName_Monthly) \
        .enableHiveSupport() \
        .getOrCreate()

    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    sqlContext = spark

    import time

    start_time = time.time()

    print "Setting LOG LEVEL as ERROR"
    sc.setLogLevel("ERROR")

    mdl_bld_date_string = "".join(sys.argv[1])

    print("Correct git branch pulled")

    print "Importing Sample Customer List"
    get_sample_customer_list(sc=sc, sqlContext=sqlContext)

    _model_bld_date_string = mdl_bld_date_string

    print("************************************************************************************")
    print (_model_bld_date_string)
    print("************************************************************************************\n")

    if p.monthly_dates.get(_model_bld_date_string):
        print("Starting Monthly Model building")
        start_time = time.time()

        build_prediction_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))

    # # Clearing cache
    # SQLContext.clearCache()

    # # Force Stopping SparkContext
    # sc.stop()

    spark.stop()
