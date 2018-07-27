from model.moving_average_weekly import moving_average_model_weekly
from model.moving_average_monthly import moving_average_model_monthly
from transform_data.pandas_support_func import get_pd_df
from transform_data.rdd_to_df import MA_output_schema, map_for_output_MA_monthly, map_for_output_MA_weekly
from transform_data.data_transform import get_weekly_aggregate
from properties import REPARTITION_STAGE_1


def _moving_average_row_to_rdd_map(line, **kwargs):
    # row_object, category_obj = line
    #
    # customernumber = row_object.customernumber
    # matnr = row_object.matnr
    #
    # if 'sep' in kwargs.keys():
    #     sep = kwargs.get('sep')
    # else:
    #     sep = "\t"
    #
    # MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # is of datetime.date type
    #
    #
    # # Unpacking the dataset
    # # Extracting only the 0th and 1st element since faced discrepancies in dataset
    # data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    #
    # data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
    #                        MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)
    #
    # data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)
    #
    # _result = customernumber, matnr, data_pd_df_week_aggregated, category_obj

    _result = line

    return _result


def _run_moving_average_weekly(test_data, sqlContext, **kwargs):
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')

    test_data_input = test_data \
        .filter(lambda x: x[3].category in ('IV','V', 'VI', 'VII', 'VIII', 'IX', 'X')) \
        .map(lambda line: _moving_average_row_to_rdd_map(line=line, MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE))

    ma_weekly_results_rdd = test_data_input \
        .repartition(REPARTITION_STAGE_1) \
        .map(lambda x: moving_average_model_weekly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[3].get_product_prop(),
                                                   weekly_window=x[3].get_window()))

    opt_ma_weekly_results_mapped = ma_weekly_results_rdd.map(lambda line: map_for_output_MA_weekly(line))

    opt_ma_weekly_results_df = sqlContext.createDataFrame(opt_ma_weekly_results_mapped, schema = MA_output_schema())

    return opt_ma_weekly_results_df


def _run_moving_average_monthly(test_data, sqlContext, **kwargs):
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # is of datetime.date type

    test_data_input = test_data \
        .filter(lambda x: x[3].category in ('VIII', 'IX', 'X')) \
        .map(lambda line: _moving_average_row_to_rdd_map(line=line, MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE))

    ma_monthly_results_rdd = test_data_input \
        .repartition(REPARTITION_STAGE_1) \
        .map(
        lambda x: moving_average_model_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[3].get_product_prop(),
                                               monthly_window=x[3].get_window()))

    opt_ma_monthly_results_mapped = ma_monthly_results_rdd.map(lambda line: map_for_output_MA_monthly(line))

    opt_ma_monthly_results_df = sqlContext.createDataFrame(opt_ma_monthly_results_mapped, schema=MA_output_schema())

    return opt_ma_monthly_results_df


if __name__ == "__main__":
    import pandas as pd
    from transform_data.data_transform import get_weekly_aggregate

    a = [['2016-09-09', '1.0', '2.0'], ['2016-09-19', '2.0', '3.0'], ['2016-10-02', '1.0', '2.0']]

    b = pd.DataFrame(data=a, columns=['date', 'q', 'q_i_p']).convert_objects(convert_numeric=True)

    print (get_weekly_aggregate(b))

    # print b
    # print type(b)
