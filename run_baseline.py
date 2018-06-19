from pyspark.sql import SparkSession
import time
import sys

if __name__ == "__main__":
    ####################################################################################################################
    print("Adding forecaster.zip to system path")
    sys.path.insert(0, "forecaster.zip")
    ####################################################################################################################

    from support_func import get_current_date, get_sample_customer_list, date_check
    from properties import MODEL_BUILDING
    import properties as p
    # from run_weekly import run_weekly
    # from run_monthly import run_monthly
    from run_weekly_baseline import run_weekly_baseline
    from run_monthly_baseline import run_monthly_baseline

    ####################################################################################################################

    # Getting Current Date Time for AppName
    appName = "_".join([MODEL_BUILDING, get_current_date()])
    ####################################################################################################################

    # conf = SparkConf()

    spark = SparkSession \
        .builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sqlContext = spark

    print ("Setting LOG LEVEL as ERROR")
    sc.setLogLevel("ERROR")

    mdl_bld_date_string = ["".join(sys.argv[1])]
    _model_bld_date_string_stg = mdl_bld_date_string[0]
    _model_bld_date_string, if_first_sunday_of_month = date_check(_model_bld_date_string_stg)

    if if_first_sunday_of_month:
        print ("Consolidated Baseline Run")
        print ("Importing Sample Customer List")

        comments = " ".join(
            ["Consolidated Baseline Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
        )

        get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string,
                                 comments=comments,
                                 module="consolidated")

        run_weekly_baseline(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        print("************************************************************************************\n")
        run_monthly_baseline(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        print("************************************************************************************\n")
    else:
        print ("Weekly Baseline Run")
        print ("Importing Sample Customer List")

        comments = " ".join(
            ["Weekly Baseline Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
        )

        get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string,
                                 comments=comments,
                                 module="weekly")
        run_weekly_baseline(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        print("************************************************************************************\n")

    # Stopping SparkContext
    spark.stop()
