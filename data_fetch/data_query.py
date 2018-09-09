import data_fetch.properties as p_data_fetch
from data_fetch.support_func import generate_weekly_query, generate_monthly_query
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def get_data_weekly(sqlContext, _bottlers, **kwargs):
    # # _bottlers is a broadcast variable
    if 'week_cutoff_date' in kwargs.keys():
        week_cutoff_date = kwargs.get('week_cutoff_date')
        test_query = generate_weekly_query(week_cutoff_date, _bottlers)
    else:
        raise ValueError

    if (test_query == None):
        raise ValueError
    else:
        test_data = \
            sqlContext.sql(test_query) \
                .filter(col('quantity') > 0) \
                .withColumn('b_date', from_unixtime(unix_timestamp(col('bill_date'), "yyyyMMdd")).cast(DateType())) \
                .withColumn('matnr_data', concat_ws("\t", col('b_date'), col('quantity'), col('q_indep_prc'))) \
                .withColumn('max_date', max(col('b_date')).over(window=Window
                                                                .partitionBy(col("customernumber"), col("matnr"))
                                                                .orderBy(col("b_date"))
                                                                .rangeBetween(Window.unboundedPreceding,
                                                                              Window.unboundedFollowing))) \
                .withColumn('min_date', min(col('b_date')).over(window=Window
                                                                .partitionBy(col("customernumber"), col("matnr"))
                                                                .orderBy(col("b_date"))
                                                                .rangeBetween(Window.unboundedPreceding,
                                                                              Window.unboundedFollowing))) \
                .withColumn('temp_curr_date', lit(week_cutoff_date)) \
                .withColumn('current_date',
                            from_unixtime(unix_timestamp(col('temp_curr_date'), "yyyy-MM-dd")).cast(DateType())) \
                .filter((datediff(col('current_date'), col('max_date')) <= p_data_fetch._latest_product_criteria_days)) \
                .withColumn('one_yr_mark', add_months(col("max_date"), -12)) \
                .withColumn('min_date_final',
                            when(col("min_date") >= col("one_yr_mark"), col("min_date")).otherwise(col("one_yr_mark"))) \
                .withColumn("consider_fr_pdt_freq",
                            when(((col("b_date") >= col("min_date_final")) & (col("b_date") <= col("max_date"))),
                                 1).otherwise(0).cast(IntegerType())) \
                .groupBy('customernumber', 'matnr') \
                .agg(collect_list('matnr_data').alias('data'),
                     min('b_date').alias('min_date_over_complete_period'),
                     max('max_date').alias('max_date'),
                     min('min_date_final').alias('min_date'),
                     sum('consider_fr_pdt_freq').cast(FloatType()).alias('invoices_in_last_one_year'),
                     ) \
                .filter(col('invoices_in_last_one_year').cast(IntegerType()) >= p_data_fetch._minimum_invoices) \
                .withColumn('temp_curr_date', lit(week_cutoff_date)) \
                .withColumn('current_date',
                            from_unixtime(unix_timestamp(col('temp_curr_date'), "yyyy-MM-dd")).cast(DateType())) \
                .withColumn('time_gap_years',
                            (datediff(col('current_date'), col('min_date_over_complete_period')).cast(
                                "int") / 365).cast(FloatType())) \
                .withColumn('time_gap_days',
                            (datediff(col('current_date'), col('min_date_over_complete_period')).cast("int")).cast(
                                FloatType())) \
                .withColumn('recent_time_gap_years',
                            (datediff(col('current_date'), col('min_date')).cast("int") / 365).cast(FloatType())) \
                .withColumn('pdt_freq_annual',
                            (col('invoices_in_last_one_year') / col('recent_time_gap_years')).cast(FloatType())) \
                .drop(col('max_date')) \
                .drop(col('min_date')) \
                .drop(col('min_date_over_complete_period')) \
                .drop(col('temp_curr_date')) \
                .drop(col('current_date')) \
                .drop(col('recent_time_gap_years')) \
                .drop(col('invoices_in_last_one_year'))

        return test_data


def get_data_monthly(sqlContext, **kwargs):
    if 'month_cutoff_date' in kwargs.keys():
        month_cutoff_date = kwargs.get('month_cutoff_date')
        test_query = generate_monthly_query(month_cutoff_date)
    else:
        raise ValueError

    if (test_query == None):
        raise ValueError
    else:
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
        # .limit(2)

        return test_data
