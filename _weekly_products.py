from data_fetch.data_query import get_data_weekly
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_arima import _run_dist_arima
from run_distributed_prophet import _run_dist_prophet
from run_moving_average import _run_moving_average_weekly
from support_func import assign_category, get_current_date
from transform_data.spark_dataframe_func import final_select_dataset
from properties import MODEL_BUILDING, weekly_pdt_cat_123_location, weekly_pdt_cat_7_location
import properties as p
from pyspark.sql.functions import *
from transform_data.data_transform import string_to_gregorian


def build_prediction_weekly(sc, sqlContext, **kwargs):
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

    print "Querying of Hive Table - Obtaining Product Data for Weekly Models"
    test_data_weekly_models = get_data_weekly(sqlContext=sqlContext, week_cutoff_date=week_cutoff_date) \
        .rdd \
        .map(lambda x: assign_category(x)) \
        .filter(lambda x: x != "NOT_CONSIDERED") \
        .filter(lambda x: x[1].category in ('I', 'II', 'III', 'VII'))

    # # Caching Data for current run
    test_data_weekly_models.cache()

    #############################________________(ARIMA + PROPHET)__________#####################################


    # Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ > 60
    print "Running WEEKLY_MODELS (ARIMA + PROPHET) on products with FREQ >= 60"
    print "\t--Running distributed arima"
    arima_results = _run_dist_arima(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                    MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    print "\t--Running distributed prophet"
    prophet_results = _run_dist_prophet(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                        MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    print "\t--Joining the ARIMA + PROPHET Results on same customernumber and matnr"
    cond = [arima_results.customernumber_arima == prophet_results.customernumber_prophet,
            arima_results.mat_no_arima == prophet_results.mat_no_prophet]

    prophet_arima_join_df = prophet_results \
        .join(arima_results, on=cond, how='outer')

    prophet_arima_join_df_select_cols = final_select_dataset(prophet_arima_join_df, sqlContext=sqlContext)

    prophet_arima_join_df_final = prophet_arima_join_df_select_cols \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('week_cutoff_date', lit(week_cutoff_date))

    print "Writing the WEEKLY_MODELS (ARIMA + PROPHET) data into HDFS"
    prophet_arima_join_df_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(weekly_pdt_cat_123_location)

    #############################________________MOVING AVERAGE__________#####################################

    print "**************\n**************"

    print "Running WEEKLY_MA_MODELS on products\n"

    ma_weekly_results_df = _run_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                                      MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    ma_weekly_results_df_final = ma_weekly_results_df \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('week_cutoff_date', lit(week_cutoff_date))

    print "Writing the MA WEEKLY data into HDFS\n"
    ma_weekly_results_df_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(weekly_pdt_cat_7_location)

    ####################################################################################################################
    # # Clearing cache before the next run
    # sqlContext.clearCache()
    test_data_weekly_models.unpersist()

    print("************************************************************************************")


if __name__ == "__main__":
    ####################################################################################################################

    # Getting Current Date Time for AppName
    appName = "_".join([MODEL_BUILDING, "W", get_current_date()])
    ####################################################################################################################

    conf = SparkConf().setAppName(appName)

    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sparkContext=sc)

    import time

    start_time = time.time()

    print "Setting LOG LEVEL as ERROR"
    sc.setLogLevel("ERROR")

    print "Add jobs.zip to system path"
    import sys

    sys.path.insert(0, "jobs.zip")

    build_prediction_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=p._model_bld_date_string_list)
    print("Time taken for running WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))
