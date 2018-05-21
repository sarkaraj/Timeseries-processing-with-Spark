from pyspark.sql import SparkSession
import time
import sys

if __name__ == "__main__":
    ####################################################################################################################
    print "Adding forecaster.zip to system path"
    sys.path.insert(0, "forecaster.zip")
    ####################################################################################################################

    from support_func import get_current_date, get_sample_customer_list, check_if_first_sunday_of_month
    from properties import MODEL_BUILDING
    import properties as p
    from run_weekly import run_weekly
    from run_monthly import run_monthly

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
    _model_bld_date_string = mdl_bld_date_string[0]

    comments = " ".join(["Consolidated Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()])

    if check_if_first_sunday_of_month(date_string=_model_bld_date_string):
        print "Importing Sample Customer List1"
        # get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string, comments=comments,
        #                          module="consolidated")
        #
        # run_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        # print("************************************************************************************\n")
        # run_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        # print("************************************************************************************\n")
    else:
        print "Importing Sample Customer List2"
        # get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string,
        #                          comments=comments,
        #                          module="weekly")
        # run_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
        # print("************************************************************************************\n")

    # Stopping SparkContext
    spark.stop()
