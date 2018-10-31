from pyspark.sql.functions import *
from pyspark.sql.types import *
from model.weekly_model_ver_1 import weekly_ensm_model
from transform_data.data_transform import get_weekly_aggregate, get_monthly_aggregate
from transform_data.pandas_support_func import *
from model.ma_outlier import *
import properties as p
import datetime


def model_fit(row_object):
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual

    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    # Obtaining weeekly aggregate
    data_pd_df = get_weekly_aggregate(data_pd_df)
    # running weekly ensemble model
    output = weekly_ensm_model(prod=data_pd_df, cus_no=customernumber, mat_no=matnr)
    # converting dataframe to list for ease of handling
    output_rdd_row = extract_from_dict_into_Row(output.to_dict(orient='index'))

    return output_rdd_row


def dist_grid_search_create_combiner(_value):
    """
    Receives tuple of structure --> (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq, value_error_counter)
    :return: (_criteria, (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq, value_error_counter))
    """
    return _value[0], _value


def dist_grid_search_merge_value(_comb_a, _value):
    """
    Merge value function for distributed arima
    :param _comb_a: (_criteria, (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq, value_error_counter))
    :param _value: (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq, value_error_counter)
    :return: (_criteria, (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq, value_error_counter))
    """
    if (_comb_a[0] > _value[0]):
        # _comb_a[0] = _value[0]
        # _comb_a[1] = _value
        # return _comb_a
        return _value[0], _value
    elif (_comb_a[0] < _value[0]):
        return _comb_a
    else:
        return _comb_a


def dist_grid_search_merge_combiner(_comb_a, _comb_b):
    """
    Combines two combiners
    :param _comb_a: (_criteria, (_criteria, output_error_dict, output_result_dict, param))
    :param _comb_b: (_criteria, (_criteria, output_error_dict, output_result_dict, param))
    :return: (_criteria, (_criteria, output_error_dict, output_result_dict, param))
    """
    if (_comb_a[0] > _comb_b[0]):
        return _comb_b
    elif (_comb_a[0] < _comb_b[0]):
        return _comb_a
    else:
        return _comb_a


def assign_category(row_object):
    if (row_object.pdt_freq_annual >= p.annual_freq_cut_1 and row_object.pdt_freq_annual < float(
            p.annual_freq_cut_MAX)):
        if (
                row_object.time_gap_days > p.cat_1.time_gap_days_lower and row_object.time_gap_days < p.cat_1.time_gap_days_upper and row_object.time_gap_years >= p.cat_1.time_gap_years):
            return row_object, p.cat_1
        elif (
                row_object.time_gap_days > p.cat_2.time_gap_days_lower and row_object.time_gap_days <= p.cat_2.time_gap_days_upper and row_object.time_gap_years >= p.cat_2.time_gap_years):
            return row_object, p.cat_2
        elif (
                row_object.time_gap_days > p.cat_3.time_gap_days_lower and row_object.time_gap_days <= p.cat_3.time_gap_days_upper and row_object.time_gap_years >= p.cat_3.time_gap_years):
            return row_object, p.cat_3
        elif (
                row_object.time_gap_days > p.cat_7.time_gap_days_lower and row_object.time_gap_days <= p.cat_7.time_gap_days_upper):
            return row_object, p.cat_7
    elif (row_object.pdt_freq_annual >= p.annual_freq_cut_2 and row_object.pdt_freq_annual < p.annual_freq_cut_1):
        if (
                row_object.time_gap_days > p.cat_4.time_gap_days_lower and row_object.time_gap_days < p.cat_4.time_gap_days_upper and row_object.time_gap_years >= p.cat_4.time_gap_years):
            return row_object, p.cat_4
        elif (
                row_object.time_gap_days > p.cat_5.time_gap_days_lower and row_object.time_gap_days <= p.cat_5.time_gap_days_upper and row_object.time_gap_years >= p.cat_5.time_gap_years):
            return row_object, p.cat_5
        elif (
                row_object.time_gap_days > p.cat_8.time_gap_days_lower and row_object.time_gap_days <= p.cat_8.time_gap_days_upper):
            return row_object, p.cat_8
    elif (row_object.pdt_freq_annual >= p.annual_freq_cut_3 and row_object.pdt_freq_annual < p.annual_freq_cut_2):
        return row_object, p.cat_9
    elif (row_object.pdt_freq_annual >= p.annual_freq_cut_MIN and row_object.pdt_freq_annual < p.annual_freq_cut_3):
        return row_object, p.cat_10
    else:
        return "NOT_CONSIDERED"

    # TODO: Testing. To be deleted later.
    # elif (
    #         row_object.time_gap_days > p.cat_6.time_gap_days_lower and row_object.time_gap_days <= p.cat_6.time_gap_days_upper and row_object.time_gap_years >= p.cat_6.time_gap_years):
    # return row_object, p.cat_6


def raw_data_to_weekly_aggregate(row_object_cat, **kwargs):
    '''
    Returns a tuple with weekly aggregated data
    :param row_object_cat: tuple of row object and category object
    :param kwargs: get model building date to add the last point to series
    :return: tuple
    '''
    if 'sep' in kwargs.keys():
        sep = kwargs.get('sep')
    else:
        sep = "\t"

    row_object, category_obj = row_object_cat
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date

    # Unpacking the dataset
    # Extracting only the 0th and 1st element since faced discrepancies in dataset
    data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
                           MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    # Obtaining weekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return customernumber, matnr, data_pd_df_week_aggregated, category_obj


def raw_data_to_monthly_aggregate(row_object_cat, **kwargs):
    '''
    Returns a tuple with monthly aggregated data
    :param row_object_cat: tuple of row object and category object
    :param kwargs: get model building date to add the last point to series
    :return: tuple
    '''
    if 'sep' in kwargs.keys():
        sep = kwargs.get('sep')
    else:
        sep = "\t"

    row_object, category_obj = row_object_cat
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date

    # Unpacking the dataset
    # Extracting only the 0th and 1st element since faced discrepancies in dataset
    data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
                           MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    # Obtaining weekly aggregate
    data_pd_df_month_aggregated = get_monthly_aggregate(data_pd_df)

    return customernumber, matnr, data_pd_df_month_aggregated, category_obj

def remove_outlier(x):
    '''
    removes the outlier based on moving average method. Before that changes the names of selected columns:
    dt_week to ds and quantity to y
    :param x: tuple (customernumber, matnr, data_pd_df_week_aggregated, category_obj, post_outlier_period_flag)
    :return: (customernumber, matnr, data_pd_df_cleaned_week_aggregated, category_obj, post_outlier_period_flag)
    '''
    from dateutil import parser

    customernumber = x[0]
    matnr = x[1]
    aggregated_data = x[2]  # weekly aggregate
    category_obj = x[3]

    aggregated_data = aggregated_data[['dt_week', 'quantity']]
    aggregated_data = aggregated_data.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

    aggregated_data.ds = aggregated_data.ds.apply(str).apply(parser.parse)
    aggregated_data.y = aggregated_data.y.apply(float)
    aggregated_data = aggregated_data.sort_values('ds')
    aggregated_data = aggregated_data.reset_index(drop=True)
    # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    # default post outlier period flag set as false
    post_outlier_period_flag = False

    # Remove outlier
    # weekly category
    if category_obj.category in ("I", "II", "III"):
        cleaned_weekly_agg_data, post_outlier_period_flag = ma_replace_outlier(data=aggregated_data, n_pass=3,
                                                                               aggressive=True, window_size=12,
                                                                               sigma=4.0)
        return customernumber, matnr, cleaned_weekly_agg_data, category_obj, post_outlier_period_flag
    # Monthly category
    elif category_obj.category in ("IV", "V", "VI", "VIII"):
        cleaned_monthly_agg_data, post_outlier_period_flag = ma_replace_outlier(data=aggregated_data, n_pass=3,
                                                                                aggressive=True, window_size=12,
                                                                                sigma=5.0)
        return customernumber, matnr, cleaned_monthly_agg_data, category_obj, post_outlier_period_flag
    # Outlier removal for moving average categories
    elif category_obj.category in ("VII"):
        if len(aggregated_data) >= 26:
            cleaned_weekly_agg_data, post_outlier_period_flag = ma_replace_outlier(data=aggregated_data, n_pass=3,
                                                                                   aggressive=True, window_size=12,
                                                                                   sigma=4.0)
            return customernumber, matnr, cleaned_weekly_agg_data, category_obj, post_outlier_period_flag
        else:
            return customernumber, matnr, aggregated_data, category_obj, post_outlier_period_flag
    elif category_obj.category in ("IX", "X"):
        if len(aggregated_data) >= 26 and category_obj.category not in ('X'):
            cleaned_monthly_agg_data, post_outlier_period_flag = ma_replace_outlier(data=aggregated_data, n_pass=3,
                                                                                    aggressive=True, window_size=24,
                                                                                    sigma=5.0)
            return customernumber, matnr, cleaned_monthly_agg_data, category_obj, post_outlier_period_flag
        else:
            return customernumber, matnr, aggregated_data, category_obj, post_outlier_period_flag


def filter_white_noise(x):
    '''
    re-assign category to filter white noise time series to cat 7
    :param x: tuple (customernumber, matnr, data_pd_df_cleaned_weekly/monthly_aggregated, category_obj)
    :return: (customernumber, matnr, data_pd_df_cleaned_weekly/monthly_aggregated, revised_product_cat)
    '''
    from statsmodels.stats import diagnostic as diag
    import numpy as np

    customernumber = x[0]
    matnr = x[1]
    cleaned_aggregated_data = x[2]  # could be monthly / weekly aggregate base on product category
    revised_product_cat_obj = x[3]

    if x[3].category in ("I", "II", "III", "IV", "V", "VI"):
        ts_data = np.array(cleaned_aggregated_data['y']).astype(float)
        try:
            lj_box_test = diag.acorr_ljungbox(ts_data, lags=10, boxpierce=False)

            min_p_val = np.nanmin(lj_box_test[1])

            if min_p_val > 0.05:
                # ts_type = "White-Noise"
                if x[3].category in ("I", "II", "III"):
                    revised_product_cat_obj = p.cat_7
                elif x[3].category in ("IV", "V", "VI"):
                    revised_product_cat_obj = p.cat_8
        except ValueError:
            print("Test Failed!")
        return customernumber, matnr, cleaned_aggregated_data, revised_product_cat_obj
    elif x[3].category in ("VII", "VIII", "IX", "X"):
        return x


def get_current_date():
    import datetime

    _date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")

    return _date


def _get_last_day_of_previous_month(_date):
    import datetime
    _first = _date.replace(day=1)
    last_month = _first - datetime.timedelta(days=1)
    return last_month.strftime("%Y-%m-%d")


def get_sample_customer_list(sc, sqlContext, **kwargs):
    customer_data_location = p.customer_data_location

    if "simulation" in kwargs.keys() and kwargs.get("simulation"):
        print(" Running Simulation get_sample_customer_list")

        if "_model_bld_date_string" in kwargs.keys():
            _model_bld_date_string = kwargs.get("_model_bld_date_string")
        else:
            print("ValueError: No model date has been provided")
            raise ValueError

        if "comments" in kwargs.keys():
            comments = kwargs.get("comments")
        else:
            comments = p.comments

        if "module" in kwargs.keys():
            module = kwargs.get("module")
            append_to_folder_name = "".join(["/", "simulation", "/", "module", "=", module])
        else:
            print("ValueError: No module date has been provided")
            raise ValueError

        if "on_demand_sales_route_location" in kwargs.keys():
            print("\'on_demand_sales_route_location\' is provided")
            print("Location provided: ", kwargs.get("on_demand_sales_route_location"))
            test_delivery_routes = kwargs.get("on_demand_sales_route_location")
        else:
            test_delivery_routes = "wasb://csotestenv@conapocv2standardsa.blob.core.windows.net/CONA_CSO/CCBCC_Consolidated/test_delivery_routes"

        # ###########################################
        # OBTAIN CUSTOMER NUMBER FROM DELIVERY ROUTES
        # ###########################################

        print("test_delivery_routes simulation")
        print(test_delivery_routes)

        _delivery_routes = sqlContext.read \
            .format("csv") \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .load(test_delivery_routes) \
            .withColumnRenamed("_c0", "sales_rep_id") \
            .select(col("sales_rep_id"))

        _complete_customer_list_from_VL_df = sqlContext.sql(
            """select * from cso_test_env.tbl_visit_list_history_complete""") \
            .select(col("USERID").alias("sales_rep_id"),
                    col("KUNNR").alias("customernumber"))

        _bottlers_stg = sqlContext.sql(
            """select userid, vkorg from cso_test_env.tbl_visit_list_history_complete""") \
            .select(col("userid").alias('sales_rep_id'),
                    col("vkorg").alias('bottler')) \
            .distinct()

        _bottlers_df = _bottlers_stg.join(broadcast(_delivery_routes),
                                          on=[_delivery_routes.sales_rep_id == _bottlers_stg.sales_rep_id],
                                          how='inner') \
            .drop(_delivery_routes.sales_rep_id) \
            .select(col("bottler")) \
            .distinct()

        _bottlers_list = [str(elem.bottler) for elem in
                          _bottlers_df.collect()]  # # is a array of string containing bottler id

        query_to_select_all_convenience_stores = """
        select customernumber
        from mdm.dim_customer
        where tradechannel = '3'
        """

        convenience_store_df = sqlContext.sql(query_to_select_all_convenience_stores) \
            .withColumnRenamed("kunnr", "customernumber")

        _custom_customer_list_df = convenience_store_df \
            .join(broadcast(_complete_customer_list_from_VL_df),
                  on=[_complete_customer_list_from_VL_df.customernumber == convenience_store_df.customernumber],
                  how="inner") \
            .drop(convenience_store_df.customernumber) \
            .join(broadcast(_delivery_routes),
                  on=[_complete_customer_list_from_VL_df.sales_rep_id == _delivery_routes.sales_rep_id],
                  how="inner") \
            .drop(_delivery_routes.sales_rep_id) \
            .drop(_complete_customer_list_from_VL_df.sales_rep_id) \
            .distinct()

        customer_sample = _custom_customer_list_df \
            .withColumn("mdl_bld_dt", lit(_model_bld_date_string)) \
            .withColumn("Comments", lit(comments))

        # if p.CUSTOMER_SAMPLING:
        #     if int(p.CUSTOMER_SAMPLING_PERCENTAGE) == 1:
        #         customer_list = customer_sample.select(col("customernumber"))
        #     else:
        #         customer_list = customer_sample.select(col("customernumber")).sample(False,
        #                                                                              p.CUSTOMER_SAMPLING_PERCENTAGE, 42)
        # else:
        customer_list = customer_sample.select(col("customernumber"))

        _bottler_broadcaster = sc.broadcast(_bottlers_list)

        customer_list.createOrReplaceTempView("customerdata")

        customer_sample \
            .write.mode('append') \
            .format('orc') \
            .option("header", "false") \
            .save(customer_data_location + append_to_folder_name)

        return _bottler_broadcaster

    else:
        print(" Running Production get_sample_customer_list")

        if "_model_bld_date_string" in kwargs.keys():
            _model_bld_date_string = kwargs.get("_model_bld_date_string")
        else:
            print("ValueError: No model date has been provided")
            raise ValueError

        if "comments" in kwargs.keys():
            comments = kwargs.get("comments")
        else:
            comments = p.comments

        if "module" in kwargs.keys():
            module = kwargs.get("module")
            append_to_folder_name = "".join(["/", "module", "=", module])
        else:
            print("ValueError: No module date has been provided")
            raise ValueError

        if "on_demand_sales_route_location" in kwargs.keys():
            print("\'on_demand_sales_route_location\' is provided")
            print("Location provided: ", kwargs.get("on_demand_sales_route_location"))
            test_delivery_routes = kwargs.get("on_demand_sales_route_location")
        else:
            test_delivery_routes = p.test_delivery_routes

        # ###########################################
        # OBTAIN CUSTOMER NUMBER FROM DELIVERY ROUTES
        # ###########################################

        print("test_delivery_routes production")
        print(test_delivery_routes)

        _delivery_routes = sqlContext.read \
            .format("csv") \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .load(test_delivery_routes) \
            .withColumnRenamed("_c0", "sales_rep_id") \
            .select(col("sales_rep_id"))

        _complete_customer_list_from_VL_df = sqlContext.read \
            .format("csv") \
            .option("delimiter", ",") \
            .option("header", "true") \
            .load(p.VISIT_LIST_LOCATION) \
            .select(col("USERID").alias("sales_rep_id"),
                    col("KUNNR").alias("customernumber"))

        _bottlers_stg = sqlContext.read \
            .format("csv") \
            .option("delimiter", ",") \
            .option("header", "true") \
            .load(p.VISIT_LIST_LOCATION) \
            .select(col("USERID").alias('sales_rep_id'),
                    col("VKORG").alias('bottler')) \
            .distinct()

        _bottlers_df = _bottlers_stg.join(broadcast(_delivery_routes),
                                          on=[_delivery_routes.sales_rep_id == _bottlers_stg.sales_rep_id],
                                          how='inner') \
            .drop(_delivery_routes.sales_rep_id) \
            .select(col("bottler")) \
            .distinct()

        _bottlers_list = [str(elem.bottler) for elem in
                          _bottlers_df.collect()]  # # is a array of string containing bottler id

        # # Todo: Delete this
        print(_bottlers_list)

        query_to_select_all_convenience_stores = """
        select customernumber
        from mdm.dim_customer
        where tradechannel = '3'
        """

        convenience_store_df = sqlContext.sql(query_to_select_all_convenience_stores) \
            .withColumnRenamed("kunnr", "customernumber")

        _custom_customer_list_df = convenience_store_df \
            .join(broadcast(_complete_customer_list_from_VL_df),
                  on=[_complete_customer_list_from_VL_df.customernumber == convenience_store_df.customernumber],
                  how="inner") \
            .drop(convenience_store_df.customernumber) \
            .join(broadcast(_delivery_routes),
                  on=[_complete_customer_list_from_VL_df.sales_rep_id == _delivery_routes.sales_rep_id],
                  how="inner") \
            .drop(_delivery_routes.sales_rep_id) \
            .drop(_complete_customer_list_from_VL_df.sales_rep_id) \
            .distinct()

        customer_sample = _custom_customer_list_df \
            .withColumn("mdl_bld_dt", lit(_model_bld_date_string)) \
            .withColumn("Comments", lit(comments))

        # if p.CUSTOMER_SAMPLING:
        #     if int(p.CUSTOMER_SAMPLING_PERCENTAGE) == 1:
        #         customer_list = customer_sample.select(col("customernumber"))
        #     else:
        #         customer_list = customer_sample.select(col("customernumber")).sample(False,
        #                                                                              p.CUSTOMER_SAMPLING_PERCENTAGE, 42)
        # else:
        #     customer_list = customer_sample.select(col("customernumber"))

        _bottler_broadcaster = sc.broadcast(_bottlers_list)

        customer_list = customer_sample.select(col("customernumber"))

        customer_list.createOrReplaceTempView("customerdata")

        customer_sample \
            .write.mode('append') \
            .format('orc') \
            .option("header", "false") \
            .save(customer_data_location + append_to_folder_name)

        return _bottler_broadcaster


def get_sample_customer_list_new_addition(sc, sqlContext, **kwargs):
    customer_data_location = p.customer_data_location

    if "simulation" in kwargs.keys() and kwargs.get("simulation"):
        print("Running Simulation get_sample_customer_list_new_addition()")

        if "_model_bld_date_string" in kwargs.keys():
            _model_bld_date_string = kwargs.get("_model_bld_date_string")
        else:
            print("ValueError: No model date has been provided")
            raise ValueError

        if "comments" in kwargs.keys():
            comments = kwargs.get("comments")
        else:
            comments = p.comments

        if "module" in kwargs.keys():
            module = kwargs.get("module")
            append_to_folder_name = "".join(
                ["/", "simulation", "/", "module", "=", module])  # # Adding folder for simulation for logging
        else:
            print("ValueError: No module date has been provided")
            raise ValueError

        if "on_demand_sales_route_location" in kwargs.keys():
            print("\'on_demand_sales_route_location\' is provided")
            print("Location provided: ", kwargs.get("on_demand_sales_route_location"))
            test_delivery_routes = kwargs.get("on_demand_sales_route_location")
        else:
            test_delivery_routes = "wasb://csotestenv@conapocv2standardsa.blob.core.windows.net/CONA_CSO/CCBCC_Consolidated/test_delivery_routes"

        # ###########################################
        # OBTAIN CUSTOMER NUMBER FROM DELIVERY ROUTES
        # ###########################################

        print("test_delivery_routes simulation")
        print(test_delivery_routes)

        _delivery_routes = sqlContext.read \
            .format("csv") \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .load(test_delivery_routes) \
            .withColumnRenamed("_c0", "sales_rep_id") \
            .select(col("sales_rep_id"))

        print("_delivery_routes_count")
        print(_delivery_routes.count())

        _complete_customer_list_from_VL_df = sqlContext.sql(
            """select * from cso_test_env.tbl_visit_list_history_complete""") \
            .select(col("USERID").alias("sales_rep_id"),
                    col("KUNNR").alias("customernumber"))

        _bottlers_stg = sqlContext.sql(
            """select userid, vkorg from cso_test_env.tbl_visit_list_history_complete""") \
            .select(col("userid").alias('sales_rep_id'),
                    col("vkorg").alias('bottler')) \
            .distinct()

        _bottlers_df = _bottlers_stg.join(broadcast(_delivery_routes),
                                          on=[_delivery_routes.sales_rep_id == _bottlers_stg.sales_rep_id],
                                          how='inner') \
            .drop(_delivery_routes.sales_rep_id) \
            .select(col("bottler")) \
            .distinct()

        _bottlers_list = [str(elem.bottler) for elem in
                          _bottlers_df.collect()]  # # is a array of string containing bottler id

        query_to_select_all_convenience_stores = """
        select customernumber
        from mdm.dim_customer
        where tradechannel = '3'
        """

        convenience_store_df = sqlContext.sql(query_to_select_all_convenience_stores) \
            .withColumnRenamed("kunnr", "customernumber")

        query_to_select_all_customers_from_last_mdl_bld_dt = """
        select customer_tbl.customernumber customernumber
        from
        (select customernumber, mdl_bld_dt
        from cso_test_env.view_consolidated_pred_complete_CCBCC
        group by customernumber, mdl_bld_dt) customer_tbl
        join
        (select max(mdl_bld_dt) mdl_bld_dt
        from cso_test_env.view_consolidated_pred_complete_CCBCC where mdl_bld_dt < """ + _model_bld_date_string + """) max_date
        on customer_tbl.mdl_bld_dt = max_date.mdl_bld_dt
        """

        customers_present_on_previous_run = sqlContext.sql(query_to_select_all_customers_from_last_mdl_bld_dt)

        _custom_customer_list_df_stg = convenience_store_df \
            .join(broadcast(_complete_customer_list_from_VL_df),
                  on=[_complete_customer_list_from_VL_df.customernumber == convenience_store_df.customernumber],
                  how="inner") \
            .drop(convenience_store_df.customernumber) \
            .join(broadcast(_delivery_routes),
                  on=[_complete_customer_list_from_VL_df.sales_rep_id == _delivery_routes.sales_rep_id],
                  how="inner") \
            .drop(_delivery_routes.sales_rep_id) \
            .drop(_complete_customer_list_from_VL_df.sales_rep_id) \
            .distinct()

        _custom_customer_list_df = _custom_customer_list_df_stg \
            .join(broadcast(customers_present_on_previous_run),
                  on=[customers_present_on_previous_run.customernumber == _custom_customer_list_df_stg.customernumber],
                  how="left") \
            .filter(isnull(customers_present_on_previous_run.customernumber)) \
            .drop(customers_present_on_previous_run.customernumber)

        _custom_customer_list_df.cache()

        if _custom_customer_list_df.count() == 0:
            # Implying there exists no new customers that has been added to the routes
            # sqlContext.clearCache()
            return False
        else:
            # Implying there exists customers that had not been present in the previous run
            customer_sample = _custom_customer_list_df \
                .withColumn("mdl_bld_dt", lit(_model_bld_date_string)) \
                .withColumn("Comments", lit(comments))

            # if p.CUSTOMER_SAMPLING:
            #     if int(p.CUSTOMER_SAMPLING_PERCENTAGE) == 1:
            #         customer_list = customer_sample.select(col("customernumber"))
            #     else:
            #         customer_list = customer_sample.select(col("customernumber")).sample(False,
            #                                                                              p.CUSTOMER_SAMPLING_PERCENTAGE,
            #                                                                              42)
            # else:

            _bottler_broadcaster = sc.broadcast(_bottlers_list)

            customer_list = customer_sample.select(col("customernumber"))

            customer_list.createOrReplaceTempView("customerdata")

            customer_sample \
                .write.mode('append') \
                .format('orc') \
                .option("header", "false") \
                .save(customer_data_location + append_to_folder_name)

            return True, _bottler_broadcaster
    else:
        print("Production get_sample_customer_list_new_addition()")

        if "_model_bld_date_string" in kwargs.keys():
            _model_bld_date_string = kwargs.get("_model_bld_date_string")
        else:
            print("ValueError: No model date has been provided")
            raise ValueError

        if "comments" in kwargs.keys():
            comments = kwargs.get("comments")
        else:
            comments = p.comments

        if "module" in kwargs.keys():
            module = kwargs.get("module")
            append_to_folder_name = "".join(["/", "module", "=", module])
        else:
            print("ValueError: No module date has been provided")
            raise ValueError

        if "on_demand_sales_route_location" in kwargs.keys():
            print("\'on_demand_sales_route_location\' is provided")
            print("Location provided: ", kwargs.get("on_demand_sales_route_location"))
            test_delivery_routes = kwargs.get("on_demand_sales_route_location")
        else:
            test_delivery_routes = p.test_delivery_routes

        # ###########################################
        # OBTAIN CUSTOMER NUMBER FROM DELIVERY ROUTES
        # ###########################################

        print("test_delivery_routes production")
        print(test_delivery_routes)

        _delivery_routes = sqlContext.read \
            .format("csv") \
            .option("delimiter", "\t") \
            .option("header", "false") \
            .load(test_delivery_routes) \
            .withColumnRenamed("_c0", "sales_rep_id") \
            .select(col("sales_rep_id"))

        print("delivery list count")
        print(_delivery_routes.count())
        # _delivery_routes.show(10)

        _complete_customer_list_from_VL_df = sqlContext.read \
            .format("csv") \
            .option("delimiter", ",") \
            .option("header", "true") \
            .load(p.VISIT_LIST_LOCATION) \
            .select(col("USERID").alias("sales_rep_id"),
                    col("KUNNR").alias("customernumber"))

        # print("_complete_customer_list_from_VL_df")
        # _complete_customer_list_from_VL_df.show(10)

        _bottlers_stg = sqlContext.read \
            .format("csv") \
            .option("delimiter", ",") \
            .option("header", "true") \
            .load(p.VISIT_LIST_LOCATION) \
            .select(col("USERID").alias('sales_rep_id'),
                    col("VKORG").alias('bottler')) \
            .distinct()

        _bottlers_df = _bottlers_stg.join(broadcast(_delivery_routes),
                                          on=[_delivery_routes.sales_rep_id == _bottlers_stg.sales_rep_id],
                                          how='inner') \
            .drop(_delivery_routes.sales_rep_id) \
            .select(col("bottler")) \
            .distinct()

        _bottlers_list = [str(elem.bottler) for elem in
                          _bottlers_df.collect()]  # # is a array of string containing bottler id

        print("bottler(s)")
        print(_bottlers_list)

        query_to_select_all_convenience_stores = """
        select customernumber
        from mdm.dim_customer
        where tradechannel = '3'
        """

        convenience_store_df = sqlContext.sql(query_to_select_all_convenience_stores) \
            .withColumnRenamed("kunnr", "customernumber")

        # print("convenience_store_df")
        # convenience_store_df.show(10)

        query_to_select_all_customers_from_last_mdl_bld_dt = """
        select customer_tbl.customernumber customernumber
        from
        (select customernumber, mdl_bld_dt
        from cso_production.view_consolidated_pred_complete_CCBCC
        group by customernumber, mdl_bld_dt) customer_tbl
        join
        (select max(mdl_bld_dt) mdl_bld_dt
        from cso_production.view_consolidated_pred_complete_CCBCC where mdl_bld_dt < """ + _model_bld_date_string + """) max_date
        on customer_tbl.mdl_bld_dt = max_date.mdl_bld_dt
        """

        customers_present_on_previous_run = sqlContext.sql(query_to_select_all_customers_from_last_mdl_bld_dt)

        _custom_customer_list_df_stg = convenience_store_df \
            .join(broadcast(_complete_customer_list_from_VL_df),
                  on=[_complete_customer_list_from_VL_df.customernumber == convenience_store_df.customernumber],
                  how="inner") \
            .drop(convenience_store_df.customernumber) \
            .join(broadcast(_delivery_routes),
                  on=[_complete_customer_list_from_VL_df.sales_rep_id == _delivery_routes.sales_rep_id],
                  how="inner") \
            .drop(_delivery_routes.sales_rep_id) \
            .drop(_complete_customer_list_from_VL_df.sales_rep_id) \
            .distinct()

        # print("_custom_customer_list_df_stg")
        # _custom_customer_list_df_stg.show(10)

        _custom_customer_list_df = _custom_customer_list_df_stg \
            .join(broadcast(customers_present_on_previous_run),
                  on=[customers_present_on_previous_run.customernumber == _custom_customer_list_df_stg.customernumber],
                  how="left") \
            .filter(isnull(customers_present_on_previous_run.customernumber)) \
            .drop(customers_present_on_previous_run.customernumber)

        _custom_customer_list_df.cache()

        # print("_custom_customer_list_df")
        # _custom_customer_list_df.show(10)

        if _custom_customer_list_df.count() == 0:
            # Implying there exists no new customers that has been added to the routes
            # sqlContext.clearCache()
            return False
        else:
            # Implying there exists customers that had not been present in the previous run
            customer_sample = _custom_customer_list_df \
                .withColumn("mdl_bld_dt", lit(_model_bld_date_string)) \
                .withColumn("Comments", lit(comments))

            # if p.CUSTOMER_SAMPLING:
            #     if int(p.CUSTOMER_SAMPLING_PERCENTAGE) == 1:
            #         customer_list = customer_sample.select(col("customernumber"))
            #     else:
            #         customer_list = customer_sample.select(col("customernumber")).sample(False,
            #                                                                              p.CUSTOMER_SAMPLING_PERCENTAGE,
            #                                                                              42)
            # else:
            #     customer_list = customer_sample.select(col("customernumber"))

            _bottler_broadcaster = sc.broadcast(_bottlers_list)

            customer_list = customer_sample.select(col("customernumber"))

            customer_list.createOrReplaceTempView("customerdata")

            customer_sample \
                .write.mode('append') \
                .format('orc') \
                .option("header", "false") \
                .save(customer_data_location + append_to_folder_name)

            return True, _bottler_broadcaster


def obtain_mdl_bld_dt():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mdl_bld_date_string", help="Model Build Date as Str in yyyy-MM-dd format")
    args = parser.parse_args()
    try:
        if args.mdl_bld_date_string:
            mdl_bld_date_string = list(args.mdl_bld_date_string)
            return mdl_bld_date_string
    except AttributeError:
        print (
            "No valid model build date has been passed as argument.\n Using Model Build Date from properties.py file.")
        mdl_bld_date_string = p._model_bld_date_string_list
        print ("\n\n")
        return mdl_bld_date_string


def string_to_gregorian(dt_str, sep='-', **kwargs):
    from datetime import date
    x = dt_str.split(sep)
    if isinstance(x[0], int) and isinstance(x[1], int) and isinstance(x[2], int):
        year = x[0]
        month = x[1]
        day = x[2]
    else:
        year = int(x[0])
        month = int(x[1])
        day = int(x[2])

    return date(year=year, month=month, day=day)


def check_if_first_sunday_of_month(_date):
    import calendar
    import __builtin__
    # _date = string_to_gregorian(date_string)
    _month = _date.month
    _year = _date.year

    sundays = [week[-1] for week in calendar.monthcalendar(year=_year, month=_month) if week[-1] != 0]
    first_sunday_of_month = __builtin__.min(sundays)
    return first_sunday_of_month == _date.day


def get_current_or_previous_sunday(d, weekday=6):
    # import datetime

    if d.weekday() == 6:
        # Checking if current day is Sunday or not. If sunday then return date as it is
        return d
    else:
        # If not sunday send previous sunday
        days_ahead = weekday - d.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        return d + datetime.timedelta(days_ahead) + datetime.timedelta(-7)


def date_check(date_string, **kwargs):
    _date = string_to_gregorian(dt_str=date_string)
    _model_bld_dt = get_current_or_previous_sunday(d=_date)
    monthly_sunday_flag = check_if_first_sunday_of_month(_model_bld_dt)
    return _model_bld_dt.strftime("%Y-%m-%d"), monthly_sunday_flag


def get_previous_sundays(_date, **kwargs):
    if "previous_weeks" in kwargs.keys():
        if isinstance(kwargs.get("previous_weeks"), int):
            previous_weeks = kwargs.get("previous_weeks")
        else:
            previous_weeks = int(kwargs.get("previous_weeks"))
    else:
        previous_weeks = 12

    start_date = string_to_gregorian(dt_str=_date)
    result_array = [(start_date + datetime.timedelta(days=(-7 * (week + 1)))).strftime("%Y-%m-%d") for week in
                    range(previous_weeks)]
    result_array.reverse()  # This step is optional. This is done just to maintain a chronology
    return result_array


if __name__ == "__main__":
    a, b = date_check("2018-06-03")
    print(a)
    print(b)
