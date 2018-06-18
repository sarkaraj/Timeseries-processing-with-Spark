from data_fetch.data_query import get_data_weekly
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_arima import _run_dist_arima
from run_distributed_prophet import _run_dist_prophet
from run_baseline_ma import _run_baseline_moving_average_weekly
from support_func import assign_category, get_current_date, raw_data_to_weekly_aggregate, remove_outlier, filter_white_noise
from transform_data.spark_dataframe_func import final_select_dataset
from properties import MODEL_BUILDING, weekly_pdt_cat_123_location_baseline
import properties as p
from pyspark.sql.functions import *
from transform_data.data_transform import string_to_gregorian


def build_baseline_prediction_weekly(sc, sqlContext, **kwargs):
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
        .map(lambda x: raw_data_to_weekly_aggregate(row_object_cat=x, MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)) \
        .map(lambda x: remove_outlier(x)) \
        .map(lambda x: filter_white_noise(x))\
        .filter(lambda x: x[3].category in ('I', 'II', 'III', 'VII'))

    # # Caching Data for current run
    test_data_weekly_models.cache()

    #############################________________MOVING AVERAGE__________#####################################

    print "**************\n**************"

    print "Running Baseline WEEKLY_MA_MODELS on product categories I, II, III\n"

    ma_weekly_results_df = _run_baseline_moving_average_weekly(test_data=test_data_weekly_models, sqlContext=sqlContext,
                                                      MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    ma_weekly_results_df_final = ma_weekly_results_df \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('week_cutoff_date', lit(week_cutoff_date))

    print "Writing the Baseline MA WEEKLY data into HDFS\n"
    ma_weekly_results_df_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(weekly_pdt_cat_123_location_baseline)

    ####################################################################################################################
    # # Clearing cache before the next run
    # sqlContext.clearCache()
    test_data_weekly_models.unpersist()

    print("************************************************************************************")

