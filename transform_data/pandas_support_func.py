def extract_from_dict(row_elem, **kwargs):
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



#
# def convert_list_to_pd_df(holidays):
#     import pandas as pd
#     import numpy as np
#     from dateutil import parser
#
#     ds = []
#     holiday = []
#     lower_window = []
#     upper_window = []
#
#     for elem in holidays:
#         ds.append(elem.ds)
#         holiday.append(elem.holiday)
#         lower_window.append(elem.lower_window)
#         upper_window.append(elem.upper_window)
#
#     ds = np.array(ds)
#     holiday = np.array(holiday)
#     lower_window = np.array(lower_window)
#     upper_window = np.array(upper_window)
#
#     ds = pd.Series(ds)
#     holiday = pd.Series(holiday)
#     lower_window = pd.Series(lower_window)
#     upper_window = pd.Series(upper_window)
#
#     holidays = pd.concat([ds, holiday, lower_window, upper_window], axis=1)
#     holidays.columns = ['ds', 'holiday', 'lower_window', 'upper_window']
#     # print holidays
#
#     holidays.ds = holidays.ds.apply(parser.parse)
#     holidays.lower_window = -7
#     holidays.upper_window = 7
#
#     # print holidays
#
#     return holidays
