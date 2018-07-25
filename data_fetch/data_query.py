import data_fetch.properties as p_data_fetch
from data_fetch.support_func import generate_weekly_query, generate_monthly_query
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def get_data_weekly(sqlContext, **kwargs):
    if 'week_cutoff_date' in kwargs.keys():
        week_cutoff_date = kwargs.get('week_cutoff_date')
        test_query = generate_weekly_query(week_cutoff_date)
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
                .withColumn('max_date_w', max(col('b_date')).over(window=Window
                                                                  .partitionBy(col("customernumber"), col("matnr"))
                                                                  .orderBy(col("b_date"))
                                                                  .rangeBetween(Window.unboundedPreceding,
                                                                                Window.unboundedFollowing))) \
                .withColumn('min_date_w', min(col('b_date')).over(window=Window
                                                                  .partitionBy(col("customernumber"), col("matnr"))
                                                                  .orderBy(col("b_date"))
                                                                  .rangeBetween(Window.unboundedPreceding,
                                                                                Window.unboundedFollowing))) \
                .withColumn('one_yr_mark', add_months(col("max_date_w"), -12)) \
                .withColumn('min_date_f', when(col("min_date_w") >= col("one_yr_mark"), col("min_date_w"))
                            .otherwise(col("one_yr_mark"))) \
                .withColumn("consider_fr_pdt_freq",
                            when(((col("b_date") >= col("min_date_f")) & (col("b_date") <= col("max_date_w"))), 1)
                            .otherwise(0).cast(IntegerType())) \
                .groupBy('customernumber', 'matnr') \
                .agg(collect_list('matnr_data').alias('data'),
                     max('b_date').alias('max_date'),
                     min('b_date').alias('min_date'),
                     max('max_date_w').alias('max_date_w'),
                     min('min_date_f').alias('min_date_w'),
                     min('one_yr_mark').alias('one_yr_mark'),
                     count('b_date').alias('row_count'),
                     sum('consider_fr_pdt_freq').alias('invoices_in_last_one_year').cast(FloatType())
                     ) \
                .withColumn('temp_curr_date', lit(week_cutoff_date)) \
                .withColumn('current_date',
                            from_unixtime(unix_timestamp(col('temp_curr_date'), "yyyy-MM-dd")).cast(DateType())) \
                .withColumn('total_time_gap_years',
                            (datediff(col('current_date'), col('min_date')).cast("int") / 365).cast(FloatType())) \
                .withColumn('total_time_gap_days',
                            (datediff(col('current_date'), col('min_date')).cast("int")).cast(FloatType())) \
                .withColumn('pdt_freq_annual_old', (col('row_count') / col('total_time_gap_years')).cast(FloatType())) \
                .withColumn('recent_time_gap_years',
                            (datediff(col('current_date'), col('min_date_w')).cast("int") / 365).cast(FloatType())) \
                .withColumn('recent_time_gap_days',
                            (datediff(col('current_date'), col('min_date_w')).cast("int")).cast(FloatType())) \
                .withColumn('pdt_freq_annual_new',
                            (col('invoices_in_last_one_year') / col('recent_time_gap_years')).cast(FloatType())) \
                .filter((datediff(col('current_date'), col('max_date')) <= p_data_fetch._latest_product_criteria_days))
        # .drop(col('max_date')) \
        # .drop(col('min_date')) \
        # .drop(col('row_count')) \
        # .drop(col('temp_curr_date')) \
        # .drop(col('current_date'))
        # .limit(2)

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
