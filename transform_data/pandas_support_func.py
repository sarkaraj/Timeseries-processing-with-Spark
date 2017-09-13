import transform_data.properties as p_transform_data


def convert_val_to_str(value):
    import math

    if(math.isnan(value)):
        return p_transform_data.NAN_REPLACEMENT
    elif(math.isinf(value)):
        return p_transform_data.INF_REPLACEMENT
    else:
        return str(value)


def extract_from_dict_into_Row(row_elem, **kwargs):
    from pyspark.sql import Row
    if(kwargs.get('multi_indexes')==True):
        return [[row_elem.get(index).get(key) for key in row_elem.get(index).keys()] for index in row_elem.keys()]

    # return [{key: row_elem.get(row_elem.keys()[0]).get(key)} for key in row_elem.get(row_elem.keys()[0]).keys()]
    _output_dict = dict((key, str(value))for key, value in row_elem.get(row_elem.keys()[0]).iteritems())

    row = Row(cus_no=_output_dict.get('cus_no'), mat_no=_output_dict.get('mat_no'), six_wre_med=_output_dict.get('6wre_med'),
        six_wre_max = _output_dict.get('6wre_max'), six_wre_med_prophet = _output_dict.get('6wre_med_prophet'),
        six_wre_max_prophet = _output_dict.get('6wre_max_prophet'), six_wre_med_arima = _output_dict.get('6wre_med_arima'),
        six_wre_max_arima = _output_dict.get('6wre_max_arima'), twelve_wre_med = _output_dict.get('12wre_med'),
        twelve_wre_max=_output_dict.get('12wre_max'), twelve_wre_med_prophet = _output_dict.get('12wre_med_prophet'),
        twelve_wre_max_prophet=_output_dict.get('12wre_max_prophet'), twelve_wre_med_arima=_output_dict.get('12wre_med_arima'),
        twelve_wre_max_arima=_output_dict.get('12wre_max_arima'), cum_error=_output_dict.get('cum_error'),
        cum_quantity=_output_dict.get('cum_quantity'), period_days=_output_dict.get('period_days'),
              rmse=_output_dict.get('rmse'), mape=_output_dict.get('mape'))
    return row


def extract_elems_from_dict(row_elem, **kwargs):
    if (kwargs.get('multi_indexes') == True):
        return [[row_elem.get(index).get(key) for key in row_elem.get(index).keys()] for index in row_elem.keys()]

    _output_dict = dict((key, convert_val_to_str(value)) for key, value in row_elem.get(row_elem.keys()[0]).iteritems())

    return _output_dict


def get_pd_df(data_array, customernumber, matnr, **kwargs):

    import pandas as pd

    data_pd_df = pd.DataFrame(data_array, columns=['date', 'quantity', 'q_indep_p']).convert_objects(
        convert_numeric=True)
    data_pd_df['customernumber'] = customernumber
    data_pd_df['matnr'] = matnr

    return data_pd_df



# a = {0: {'a':float('nan'), 'b':float('inf')}}
#
# print extract_elems_from_dict(a)
#
# print dict([('a', 1), ('b', 2)])