# from pyspark.sql.types import *
#
# def _which_prophet_module():
#     import fbprophet
#     import pystan
#     _result = str(fbprophet.__file__), str(pystan.__file__)
#     return _result
#
#
# def _schema():
#     proph = StructField("fb_loc", StringType())
#     pystan = StructField("pystan_loc", StringType())
#     _result = StructType([proph, pystan])
#     return _result


def _run_dist_prophet_monthly(test_data, sqlContext, **kwargs):
    # LIBRARY IMPORTS
    from distributed_grid_search._model_params_set import generate_models_prophet_monthly
    from transform_data.rdd_to_df import map_for_output_prophet, prophet_output_schema

    from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, \
        dist_grid_search_merge_combiner

    from distributed_grid_search._fbprophet_monthly import run_prophet_monthly
    from properties import REPARTITION_STAGE_1, REPARTITION_STAGE_2

    # ###################

    test_data_input = test_data \
        .filter(lambda x: x[1].category in ('IV', 'V', 'VI'))

    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')

    test_data_parallel = test_data_input.flatMap(
        lambda x: generate_models_prophet_monthly(x,
                                                  MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE))  # # gets 587 * 55 = 32285 rows

    test_data_parallel.cache()
    test_data_parallel.take(1)

    # # Parallelizing Jobs
    prophet_results_rdd = test_data_parallel \
        .repartition(REPARTITION_STAGE_1) \
        .map(lambda x: run_prophet_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], param=x[3],
                                           min_train_days=x[4].min_train_days, pdt_cat=x[4].get_product_prop())) \
        .filter(lambda x: x != "MODEL_NOT_VALID")
    # .repartition(REPARTITION_STAGE_1)

    test_data_parallel.unpersist()

    opt_prophet_results_rdd = prophet_results_rdd \
        .combineByKey(dist_grid_search_create_combiner,
                      dist_grid_search_merge_value,
                      dist_grid_search_merge_combiner)

    opt_prophet_results_mapped = opt_prophet_results_rdd.map(lambda line: map_for_output_prophet(line))

    opt_prophet_results_df = sqlContext.createDataFrame(opt_prophet_results_mapped, schema=prophet_output_schema())

    # return opt_prophet_results_mapped
    return opt_prophet_results_df



# FOR MODULE TESTING

if __name__ == "__main__":
    from pyspark.sql import HiveContext, SparkSession, SQLContext
    import data_fetch.properties as p_data_fetch
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    import properties as p
    from transform_data.data_transform import string_to_gregorian
    from run_distributed_prophet_monthly import _run_dist_prophet_monthly
    from support_func import assign_category

    # from data_fetch.data_query import get_data_weekly, get_data_monthly
    # from pyspark import SparkContext, SparkConf
    # from pyspark.sql import HiveContext
    # from run_distributed_arima import _run_dist_arima
    # from run_distributed_prophet import _run_dist_prophet
    # from run_moving_average import _run_moving_average_weekly, _run_moving_average_monthly

    # from transform_data.spark_dataframe_func import final_select_dataset

    # conf = SparkConf().setAppName("CONA_TS_MODEL_VALIDATION_JOB_ID_15")
    # # .setMaster("yarn-client")
    # sc = SparkContext(conf=conf)
    # sqlContext = HiveContext(sparkContext=sc)

    ###################################################################################################################

    # Getting Current Date Time for AppName
    appName_Monthly = "CONA_TS_MODEL_VALIDATION_JOB_ID_15"
    ####################################################################################################################

    # conf = SparkConf().setAppName(appName)
    #
    # sc = SparkContext(conf=conf)
    # sqlContext = HiveContext(sparkContext=sc)

    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir",
                "/home/rajarshi/spark-warehouse") \
        .appName(appName_Monthly) \
        .enableHiveSupport() \
        .getOrCreate()

    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    sqlContext = spark

    import time

    start_time = time.time()

    print "Setting LOG LEVEL as ERROR"
    sc.setLogLevel("ERROR")

    MODEL_BLD_CURRENT_DATE = string_to_gregorian("2018-04-01")
    _model_bld_date_string = "2018-04-01"
    month_cutoff_date = '2018-03-31'
    temp_test_location = "/home/rajarshi/Desktop/temporary/temporary_spark_file_dump"

    raw_dataset_1 = spark.read.format('csv') \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .load("/home/rajarshi/Documents/CONA_LINUX/thaddeusSmithRawInvoice")

    raw_dataset_1.cache()

    file_from = open("/home/rajarshi/Desktop/temporary/thaddeusSmithCustomerList/Incomplete.txt", mode="r")
    file_to = open("/home/rajarshi/Desktop/temporary/thaddeusSmithCustomerList/Complete.txt", mode="a")

    for customernumber in file_from.readlines():
        customernumber_clean = "".join([elem for elem in customernumber if elem != "\n"])
        customernumber_complete = str(0) + str(customernumber_clean)

        raw_dataset = raw_dataset_1 \
            .filter(col("customernumber") == customernumber_complete)

        file_to.write(customernumber_complete + "\n")

        # raw_dataset.show(10)

        filtered_dataset = raw_dataset \
            .filter(col('quantity') > 0) \
            .withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
            .withColumn('matnr_data', concat_ws("\t", col('b_date'), col('quantity'), col('q_indep_prc'))) \
            .groupBy('customernumber', 'matnr') \
            .agg(collect_list('matnr_data').alias('data'),
                 max('b_date').alias('max_date'),
                 min('b_date').alias('min_date'),
                 count('b_date').alias('row_count')) \
            .withColumn('temp_curr_date', lit(month_cutoff_date)) \
            .withColumn('current_date',
                        from_unixtime(unix_timestamp(col('temp_curr_date'), "yyyy-MM-dd")).cast(DateType())) \
            .withColumn('time_gap_years',
                        (datediff(col('current_date'), col('min_date')).cast("int") / 365).cast(FloatType())) \
            .withColumn('time_gap_days',
                        (datediff(col('current_date'), col('min_date')).cast("int")).cast(FloatType())) \
            .withColumn('pdt_freq_annual', (col('row_count') / col('time_gap_years')).cast(FloatType())) \
            .filter((datediff(col('current_date'), col('max_date')) <= p_data_fetch._latest_product_criteria_days)) \
            .drop(col('max_date')) \
            .drop(col('min_date')) \
            .drop(col('row_count')) \
            .drop(col('temp_curr_date')) \
            .drop(col('current_date'))

        # filtered_dataset.show(10)

        assigned_category = filtered_dataset.rdd \
            .map(lambda x: assign_category(x)) \
            .filter(lambda x: x != "NOT_CONSIDERED") \
            .filter(lambda x: x[1].category in ('IV', 'V', 'VI'))

        # .filter(lambda x: x[1].category in ('IV', 'V', 'VI', 'VIII', 'IX', 'X'))

        # print(assigned_category.count())

        print ("Running MONTHLY_MODELS PROPHET on products with FREQ : " + str(p.annual_freq_cut_2) + " <= X < "
               + str(p.annual_freq_cut_1) + "\n")

        print "\t\t--Running distributed prophet"
        prophet_monthly_results = _run_dist_prophet_monthly(test_data=assigned_category, sqlContext=sqlContext,
                                                            MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

        # prophet_monthly_results.show(10)

        prophet_monthly_results_final = prophet_monthly_results \
            .withColumn('mdl_bld_dt', lit(_model_bld_date_string)) \
            .withColumn('month_cutoff_date', lit(month_cutoff_date))

        print("Printing prophet_monthly_results_final")
        # prophet_monthly_results_final.show(10)

        print "Writing the MONTHLY MODEL data into HDFS"
        prophet_monthly_results_final \
            .write.mode('append') \
            .format('orc') \
            .option("header", "true") \
            .save(temp_test_location)

    # #############################________________PROPHET__________################################
    #
    # print "**************\n**************\n"
    #
    # # Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60
    # print "Running MONTHLY_MODELS PROPHET on products with FREQ : 20 <= X < 60\n"
    # # print "\t\t--Running distributed prophet"
    # prophet_monthly_results = _run_dist_prophet_monthly(test_data=test_data_monthly_model, sqlContext=sqlContext)
    #
    # prophet_monthly_results.distinct().show()
    #
    # # # print prophet_monthly_results
    # #
    # # print "Writing the MONTHLY MODEL data into HDFS"
    # # prophet_monthly_results.coalesce(4).write.mode('overwrite').format('orc').option("header", "false").save(
    # #     "/tmp/pyspark_data/dist_model_monthly_first_run_testing")

    # print(sc)
    # print(sqlContext)

    raw_dataset_1.unpersist()

    file_from.close()
    file_to.close()
    spark.stop()
