from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSession, SQLContext
from support_func import get_current_date, get_sample_customer_list, obtain_mdl_bld_dt
from properties import MODEL_BUILDING
from _weekly_baseline import build_baseline_prediction_weekly
import properties as p
# from _monthly_products import build_prediction_monthly
import time


def run_weekly_baseline(sc, sqlContext, _model_bld_date_string):
    print("************************************************************************************")
    print (_model_bld_date_string)
    print("************************************************************************************\n")
    print("Starting Weekly Baseline Model building")
    start_time = time.time()

    build_baseline_prediction_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    print("Time taken for running Baseline WEEKLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))