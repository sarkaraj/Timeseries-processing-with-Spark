from pyspark.sql import SparkSession
from _monthly_products import build_prediction_monthly
import time
import sys


def run_monthly(sc, sqlContext, _model_bld_date_string):
    print("************************************************************************************")
    print (_model_bld_date_string)
    print("************************************************************************************\n")
    print("Starting Monthly Model building")
    start_time = time.time()

    build_prediction_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    print("Time taken for running MONTHLY MODELS:\t\t--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    from support_func import get_current_date, get_sample_customer_list, date_check
    from properties import MODEL_BUILDING
    import properties as p

    # ###################################################################################################################
    # Getting Current Date Time for AppName
    appName = "_".join([MODEL_BUILDING, "MONTHLY", get_current_date()])
    # ###################################################################################################################

    # conf = SparkConf()

    # ###################################################################################################################
    print("Adding forecaster.zip to system path")
    sys.path.insert(0, "forecaster.zip")
    # ###################################################################################################################
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    sqlContext = spark

    print ("Setting LOG LEVEL as ERROR")
    sc.setLogLevel("ERROR")

    mdl_bld_date_string = ["".join(sys.argv[1])]
    _model_bld_date_string_stg = mdl_bld_date_string[0]
    _model_bld_date_string, if_first_sunday_of_month = date_check(_model_bld_date_string_stg)

    if if_first_sunday_of_month:
        print ("Monthly Run")
        print ("Importing Sample Customer List")

        comments = " ".join(
            ["Monthly Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
        )

        get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string,
                                 comments=comments,
                                 module="monthly")
        run_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        print("************************************************************************************\n")
    else:
        print("Error. Not the first sunday of the month.")
        raise ValueError

    # Stopping SparkContext
    spark.stop()
