from data_fetch.data_query import get_data_monthly
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from run_distributed_prophet_monthly import _run_dist_prophet_monthly
from run_baseline_ma import _run_baseline_moving_average_monthly
from support_func import assign_category, get_current_date, _get_last_day_of_previous_month
from properties import MODEL_BUILDING, monthly_pdt_cat_456_location_baseline, monthly_pdt_cat_8910_location
import properties as p
from pyspark.sql.functions import *
from transform_data.data_transform import string_to_gregorian


def build_baseline_prediction_monthly(sc, sqlContext, **kwargs):
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



    print "Querying of Hive Table - Obtaining Product Data for Monthly Models"
    test_data_monthly_model = get_data_monthly(sqlContext=sqlContext, month_cutoff_date=month_cutoff_date) \
        .map(lambda x: assign_category(x)) \
        .filter(lambda x: x != "NOT_CONSIDERED") \
        .filter(lambda x: x[1].category in ('IV', 'V', 'VI'))

    # # Caching Data for this run
    test_data_monthly_model.cache()

    ############################________________MOVING AVERAGE__________##########################

    print "**************\n**************\n"

    print "Running MONTHLY_MA_MODELS on products\n"

    ma_monthly_results_df = _run_baseline_moving_average_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext,
                                                        MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    ma_monthly_results_df_final = ma_monthly_results_df \
        .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
        .withColumn('month_cutoff_date', lit(month_cutoff_date))

    print "Writing the MA MONTHLY data into HDFS\n"
    ma_monthly_results_df_final \
        .write.mode('append') \
        .format('orc') \
        .option("header", "false") \
        .save(monthly_pdt_cat_456_location_baseline)

    # # Clearing cache
    # sqlContext.clearCache()
    test_data_monthly_model.unpersist()

    print("************************************************************************************")


