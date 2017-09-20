from model.weekly_model_ver_1 import weekly_ensm_model
from transform_data.data_transform import get_weekly_aggregate
from transform_data.pandas_support_func import *
import properties as p

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
    if(_comb_a[0] > _value[0]):
        # _comb_a[0] = _value[0]
        # _comb_a[1] = _value
        # return _comb_a
        return _value[0], _value
    elif(_comb_a[0] < _value[0]):
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
    if(_comb_a[0] > _comb_b[0]):
        return _comb_b
    elif(_comb_a[0] < _comb_b[0]):
        return _comb_a
    else:
        return _comb_a


def assign_category(row_object):
    if (row_object.pdt_freq_annual >= 60 and row_object.pdt_freq_annual < float('inf')):
        if (
                            row_object.time_gap_days >= p.cat_1.time_gap_days_lower and row_object.time_gap_days < p.cat_1.time_gap_days_upper and row_object.time_gap_years >= p.cat_1.time_gap_years):
            return row_object, p.cat_1
        elif (
                            row_object.time_gap_days >= p.cat_2.time_gap_days_lower and row_object.time_gap_days < p.cat_2.time_gap_days_upper and row_object.time_gap_years >= p.cat_2.time_gap_years):
            return row_object, p.cat_2
        elif (
                            row_object.time_gap_days >= p.cat_3.time_gap_days_lower and row_object.time_gap_days < p.cat_3.time_gap_days_upper and row_object.time_gap_years >= p.cat_3.time_gap_years):
            return row_object, p.cat_3
        else:
            # TODO Change return to row_object, p.cat_7
            return "NOT_CONSIDERED"
            # return row_object, p.cat_7
    elif (row_object.pdt_freq_annual >= 20 and row_object.pdt_freq_annual < 60):
        if (
                            row_object.time_gap_days >= p.cat_4.time_gap_days_lower and row_object.time_gap_days < p.cat_4.time_gap_days_upper and row_object.time_gap_years >= p.cat_4.time_gap_years):
            return row_object, p.cat_4
        elif (
                            row_object.time_gap_days >= p.cat_5.time_gap_days_lower and row_object.time_gap_days < p.cat_5.time_gap_days_upper and row_object.time_gap_years >= p.cat_5.time_gap_years):
            return row_object, p.cat_5
        elif (
                            row_object.time_gap_days >= p.cat_6.time_gap_days_lower and row_object.time_gap_days < p.cat_6.time_gap_days_upper and row_object.time_gap_years >= p.cat_6.time_gap_years):
            return row_object, p.cat_6
        else:
            # TODO Change return to row_object, p.cat_8
            return "NOT_CONSIDERED"
            # return row_object, p.cat_8
    else:
        return "NOT_CONSIDERED"
