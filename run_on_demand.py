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
    appName = "_".join([MODEL_BUILDING, "_ON_DEMAND_EXEC_", get_current_date()])
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

    try:
        mdl_bld_date_string = ["".join(sys.argv[1])]
        _model_bld_date_string_stg = mdl_bld_date_string[0]

        on_demand_sales_route_location = ["".join(sys.argv[2])][0]  # # this is a string

        print("_model_bld_date_string_stg")
        print(_model_bld_date_string_stg)
        print("on_demand_sales_route_location")
        print(on_demand_sales_route_location)

    except IndexError:
        print("IndexError: sys.argv does not contain enough arguments for executing \'run_on_demand\'.")
        print("\'run_on_demand\' requires model_bld_dt and on_demand_sales_route_location")
        print("Provide both the arguments in order with \'spark-submit\' command before executing.")
        raise IndexError

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
                                                           module="consolidated",
                                                           on_demand_sales_route_location=on_demand_sales_route_location)

    if isinstance(new_cust_check, bool):
        pass
    else:
        if isinstance(new_cust_check, tuple) and len(new_cust_check) == 2 and new_cust_check[
            0] is True:  # # When len(new_cust_check) is 1 --> False
            print("Backlog Run")
            # New customers are present
            # Running predictor for generating predictions for previous weeks : CURRENTLY FOR SLOW PRODUCTS

            temp_df = sqlContext.sql("""select * from customerdata""")

            print("TEMP_DF count")
            print(temp_df.count())
            # print("TEMP_DF Sample customers")
            # temp_df.show(10)

            all_previous_sundays = get_previous_sundays(_date=_model_bld_date_string)  # this is an array

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
                                                      module="weekly",
                                                      on_demand_sales_route_location=on_demand_sales_route_location)

    run_weekly(sc=sc,
               sqlContext=sqlContext,
               _model_bld_date_string=_model_bld_date_string,
               _bottlers=_bottler_broadcaster_2)
    print("************************************************************************************\n")

    # Cleaning Up memory
    sqlContext.catalog.dropTempView("customerdata")

    # Stopping SparkContext
    spark.stop()
