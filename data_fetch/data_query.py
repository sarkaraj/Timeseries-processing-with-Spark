import properties as p


def getData(sqlContext, test_query=p.query, _latest_product_criteria_days=p._latest_product_criteria_days,
            _product_criteria_years=p._product_criteria_years, _product_criteria_days=p._product_criteria_days,
            _product_criteria_annual_frequency=p._product_criteria_annual_frequency):
    if (test_query == None):
        return "Empty Query"
    else:
        from pyspark.sql.functions import *
        from pyspark.sql.types import *

        test_data = \
            sqlContext.sql(test_query) \
                .filter(col('quantity') > 0) \
                .withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
                .withColumn('matnr_data', concat_ws("\t", col('b_date'), col('quantity'), col('q_indep_prc'))) \
                .groupBy('customernumber', 'matnr') \
                .agg(collect_list('matnr_data').alias('data'),
                     max('b_date').alias('max_date'),
                     min('b_date').alias('min_date'),
                     count('b_date').alias('row_count')) \
                .withColumn('time_gap_years',
                            (datediff(col('max_date'), col('min_date')).cast("int") / 365).cast(FloatType())) \
                .withColumn('time_gap_days', (datediff(col('max_date'), col('min_date')).cast("int")).cast(FloatType())) \
                .withColumn('current_date', current_date()) \
                .filter((datediff(col('current_date'), col('max_date')) <= _latest_product_criteria_days) & (
            col('time_gap_years') >= _product_criteria_years) & (
                            col('time_gap_days') >= _product_criteria_days)) \
                .withColumn('pdt_freq_annual', (col('row_count') / col('time_gap_years')).cast(FloatType())) \
                .filter((col('pdt_freq_annual') >= _product_criteria_annual_frequency)) \
                .drop(col('max_date')) \
                .drop(col('min_date')) \
                .drop(col('row_count')) \
                .drop(col('time_gap_years')) \
                .drop(col('time_gap_days')) \
                .drop(col('current_date')) \
                .limit(8)


        # .filter((col('pdt_freq_annual') >= 12) & (col('pdt_freq_annual') <= 60))\
        # .drop(col('pdt_freq_annual'))\
        # .select('customernumber', 'matnr', explode('data').alias('data'))\
        # .withColumn('bill_date', split(col('data'), "\t").getItem(0).cast(StringType()))\
        # .withColumn('quantity', split(col('data'), "\t").getItem(1).cast(DoubleType()))\
        # .withColumn('q_indep_prc', split(col('data'), "\t").getItem(2).cast(DoubleType()))\
        # # .withColumn('month', month(col('bill_date')))\
        # # .withColumn('year', year(col('bill_date')))\
        # .drop(col('data'))

        # test_data.show()
        # test_data.cache()


        # test_data.coalesce(1).write.mode('overwrite').format('orc').option("header", "false").save("/tmp/pyspark_data/25_stores_freq_12_btwn_60_weekly")

        # debopriya = """create external table if not exists posdb.debs(
        # customernumber string,
        # matnr string,
        # bill_date string,
        # quantity double,
        # q_indep_prc double
        # )
        # stored as orc
        # location '/tmp/pyspark_data/25_stores_freq_12_btwn_60_weekly'
        # """

        # sqlContext.sql(debopriya)

        # print test_data.printSchema

        return test_data
