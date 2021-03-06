from pyspark.sql import SparkSession
import time
import sys

if __name__ == "__main__":
    # ###################################################################################################################
    print("Adding forecaster.zip to system path")
    sys.path.insert(0, "forecaster.zip")
    # ###################################################################################################################

    from support_func import get_current_date, get_sample_customer_list, date_check, \
        get_sample_customer_list_new_addition, get_previous_sundays
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

    print("Setting LOG LEVEL as ERROR")
    sc.setLogLevel("ERROR")

    mdl_bld_date_string = ["".join(sys.argv[1])]
    _model_bld_date_string_stg = mdl_bld_date_string[0]
    _model_bld_date_string, if_first_sunday_of_month = date_check(_model_bld_date_string_stg)

    # ###############################################################################################
    # Check for new customers and generate predictions for previous 12 weeks

    comments = " ".join(
        ["Backlog Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
    )

    new_cust_check = get_sample_customer_list_new_addition(sc=sc,
                                                           sqlContext=sqlContext,
                                                           _model_bld_date_string=_model_bld_date_string,
                                                           comments=comments,
                                                           module="consolidated")
    if isinstance(new_cust_check, bool):
        print("INFO: No new customers have been added to any of the routes.")
        pass
    else:
        if isinstance(new_cust_check, tuple) and len(new_cust_check) == 2 and new_cust_check[
            0] is True:  # # When len(new_cust_check) is 1 --> False
            print("INFO: New customers have been added to existing routes.")
            print("Starting Backlog Run")
            # New customers are present
            # Running predictor for generating predictions for previous weeks : CURRENTLY FOR SLOW PRODUCTS

            temp_df = sqlContext.sql("""select * from customerdata""")

            print("new customers count")
            print(temp_df.count())

            all_previous_sundays = get_previous_sundays(_date=_model_bld_date_string)  # this is an array

            print("Running backlog for the past %d week(s)." % len(all_previous_sundays))

            _bottler_broadcaster_1 = new_cust_check[1]  # # accessing the broadcaster variable for bottler id's

            for sunday in all_previous_sundays:
                print("**********************************" + sunday + "************************************\n")
                run_weekly(sc=sc,
                           sqlContext=sqlContext,
                           _model_bld_date_string=sunday,
                           backlog=True,
                           _bottlers=_bottler_broadcaster_1)
                print("************************************************************************************\n")

            sqlContext.catalog.dropTempView("customerdata")

    # # Running normal weekly runs
    print("Weekly Run")
    print("Importing Sample Customer List")

    comments = " ".join(
        ["Weekly Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
    )

    _bottler_broadcaster_2 = get_sample_customer_list(sc=sc,
                                                      sqlContext=sqlContext,
                                                      _model_bld_date_string=_model_bld_date_string,
                                                      comments=comments,
                                                      module="weekly")

    run_weekly(sc=sc,
               sqlContext=sqlContext,
               _model_bld_date_string=_model_bld_date_string,
               _bottlers=_bottler_broadcaster_2)
    print("************************************************************************************\n")

    # if if_first_sunday_of_month:
    #     print("Consolidated Run")
    #     print("Importing Sample Customer List")
    #
    #     comments = " ".join(
    #         ["Consolidated Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
    #     )
    #
    #     get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string,
    #                              comments=comments,
    #                              module="consolidated")
    #
    #     run_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    #     print("************************************************************************************\n")
    #     run_monthly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    #     print("************************************************************************************\n")
    # else:
    #     print("Weekly Run")
    #     print("Importing Sample Customer List")
    #
    #     comments = " ".join(
    #         ["Weekly Run. Dated:", str(_model_bld_date_string), "Execution-Date", get_current_date()]
    #     )
    #
    #     get_sample_customer_list(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string,
    #                              comments=comments,
    #                              module="weekly")
    #     run_weekly(sc=sc, sqlContext=sqlContext, _model_bld_date_string=_model_bld_date_string)
    #     print("************************************************************************************\n")

    # Cleaning Up memory
    sqlContext.catalog.dropTempView("customerdata")

    # Stopping SparkContext
    spark.stop()
