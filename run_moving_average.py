from model.moving_average_weekly import moving_average_model_weekly
from model.moving_average_monthly import moving_average_model_monthly
from transform_data.pandas_support_func import get_pd_df
from transform_data.rdd_to_df import MA_output_schema, map_for_output_MA_monthly, map_for_output_MA_weekly
from transform_data.data_transform import get_weekly_aggregate


def _moving_average_row_to_rdd_map(line):
    row_object, category_obj = line

    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual


    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    _result = customernumber, matnr, data_pd_df_week_aggregated, category_obj

    return _result


def _run_moving_average(test_data, sqlContext):
    test_data_input = test_data \
        .filter(lambda x: x[1].category in ['VII', 'VIII']) \
        .map(lambda line: _moving_average_row_to_rdd_map(line=line))

    # test_data_parallel = test_data_input.flatMap(lambda x: generate_models_prophet_monthly(x))

    ma_weekly_results_rdd = test_data_input.filter(lambda x: x[3].category == 'VII') \
        .map(
        lambda x: moving_average_model_weekly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[3].get_product_prop()))
    # \.filter(lambda x: x != "MODEL_NOT_VALID")

    ma_monthly_results_rdd = test_data_input.filter(lambda x: x[3].category == 'VIII') \
        .map(
        lambda x: moving_average_model_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[3].get_product_prop()))
    # .filter(lambda x: x != "MODEL_NOT_VALID")



    opt_ma_weekly_results_mapped = ma_weekly_results_rdd.map(lambda line: map_for_output_MA_weekly(line))
    opt_ma_monthly_results_mapped = ma_monthly_results_rdd.map(lambda line: map_for_output_MA_monthly(line))

    opt_ma_weekly_results_df = sqlContext.createDataFrame(opt_ma_weekly_results_mapped, schema=MA_output_schema())
    opt_ma_monthly_results_df = sqlContext.createDataFrame(opt_ma_monthly_results_mapped, schema=MA_output_schema())

    return opt_ma_weekly_results_df, opt_ma_monthly_results_df


def _run_moving_average_monthly(test_data, sqlContext):
    test_data_input = test_data \
        .filter(lambda x: x[1].category in ['IX']) \
        .map(lambda line: _moving_average_row_to_rdd_map(line=line))

    # test_data_parallel = test_data_input.flatMap(lambda x: generate_models_prophet_monthly(x))
    #
    # ma_weekly_results_rdd = test_data_input.filter(lambda x: x[3].category == 'VII') \
    #     .map(
    #     lambda x: moving_average_model_weekly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[3].get_product_prop()))
    # # \.filter(lambda x: x != "MODEL_NOT_VALID")

    ma_monthly_results_rdd = test_data_input \
        .map(
        lambda x: moving_average_model_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[3].get_product_prop()))
    # .filter(lambda x: x != "MODEL_NOT_VALID")



    # opt_ma_weekly_results_mapped = ma_weekly_results_rdd.map(lambda line: map_for_output_MA_weekly(line))
    opt_ma_monthly_results_mapped = ma_monthly_results_rdd.map(lambda line: map_for_output_MA_monthly(line))

    # opt_ma_weekly_results_df = sqlContext.createDataFrame(opt_ma_weekly_results_mapped, schema=MA_output_schema())
    opt_ma_monthly_results_df = sqlContext.createDataFrame(opt_ma_monthly_results_mapped, schema=MA_output_schema())

    # return opt_ma_weekly_results_df, opt_ma_monthly_results_df
    return opt_ma_monthly_results_df
