from pyspark.sql import SparkSession
from support_func import get_current_date, get_sample_customer_list, obtain_mdl_bld_dt
from properties import MODEL_BUILDING
import properties as p
from _monthly_baseline import build_baseline_prediction_monthly
import time


def run_monthly_baseline(sc, sqlContext, _model_bld_date_string):
    print("************************************************************************************")
    print (_model_bld_date_string)
    print("************************************************************************************\n")
    print("Starting Monthly Baseline Model building")
    start_time = time.time()

    build_baseline_prediction_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    print("Time taken for running MONTHLY Baseline MODEL:\t\t--- %s seconds ---" % (time.time() - start_time))