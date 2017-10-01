import properties as p_data_fetch


def get_data_weekly(sqlContext, **kwargs):
    test_query = p_data_fetch.query_weekly

    if (test_query == None):
        raise ValueError
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
                .withColumn('pdt_freq_annual', (col('row_count') / col('time_gap_years')).cast(FloatType())) \
                .filter((datediff(col('current_date'), col('max_date')) <= p_data_fetch._latest_product_criteria_days)) \
                .drop(col('max_date')) \
                .drop(col('min_date')) \
                .drop(col('row_count')) \
                .drop(col('current_date'))
        # .limit(2)

        return test_data


def get_data_monthly(sqlContext, **kwargs):
    test_query = p_data_fetch.query_monthly

    if (test_query == None):
        raise ValueError
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
                .withColumn('pdt_freq_annual', (col('row_count') / col('time_gap_years')).cast(FloatType())) \
                .filter((datediff(col('current_date'), col('max_date')) <= p_data_fetch._latest_product_criteria_days)) \
                .drop(col('max_date')) \
                .drop(col('min_date')) \
                .drop(col('row_count')) \
                .drop(col('current_date'))
        # .limit(2)

        return test_data
