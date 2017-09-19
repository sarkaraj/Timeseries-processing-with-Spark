# from pyspark.sql.types import *


def map_for_output_prophet(line):
    customernumber, mat_no = line[0]
    _error_prophet = line[1][1][1]
    _req_error_prophet_param = {key: float(_error_prophet.get(key)) for key in _error_prophet.keys() if key not in ('mat_no', 'cus_no')}
    _pred_prophet = line[1][1][2]
    _opt_param = line[1][1][3]
    _opt_param_prophet = {key:str(_opt_param.get(key)) for key in _opt_param.keys()}

    _result = customernumber, mat_no, _req_error_prophet_param, _pred_prophet, _opt_param_prophet
    return _result


def rdd_to_df_prophet(_data):
    # TODO: Currently needs bugs fixing.
    from pyspark.sql import Row
    customernumber, mat_no = _data[0]
    _error_prophet = _data[1][1][1]
    _pred_prophet = _data[1][1][3]
    _opt_param_prophet = _data[1][1][4]


    _Result = Row(customernumber=customernumber, mat_no=mat_no, period_days_prophet=_error_prophet.get('period_days'),
                  cum_quantity=_error_prophet.get('cum_quantity'), wre_max_6_prophet=_error_prophet.get('wre_max_6'),
                  rmse_prophet=_error_prophet.get('rmse'), wre_med_12_prophet=_error_prophet.get('wre_med_12'),
                  mape_prophet=_error_prophet.get('mape'), wre_med_6_prophet=_error_prophet.get('wre_med_6'),
                  cum_error_prophet=_error_prophet.get('cum_error'),
                  wre_max_12_prophet=_error_prophet.get('wre_max_12'), yhat_prophet=_pred_prophet.get('yhat'),
                  param=_opt_param_prophet)

    return _Result


def prophet_output_schema():
    """
    Expected dataframe structure for the specifiec schema is
    (customernumber, mat_no, _error_prophet(in dictionary format --> {string -> float}),
    _pred_prophet(as a dictionary {string -> array(float)}), param(as dictionary {string -> string}))

    Example:::
    (u'0500083147', u'000000000000132218', {'period_days': 154, 'cum_quantity': 35.54545454545455, 'wre_max_6': 38.2428908091008, 'rmse': 0.63, 'wre_med_12': 12.635990296609734, 'mape': 40.69, 'mat_no': u'000000000000132218', 'wre_med_6': 6.86067324942004, 'cum_error': -0.8573985749159931, 'wre_max_12': 16.296254405296658, 'cus_no': u'0500083147'}, {u'yhat': [1.6055120386730186]}, {'yearly_seasonality': False, 'changepoint_prior_scale': 3, 'holidays': False})
    :return: schema of Row(customernumber, mat_no, error_prophet, pred_prophet, prophet_params)
    """

    customernumber = StructField("customernumber", StringType(), nullable=False)
    mat_no = StructField("mat_no", StringType(), nullable=False)
    _error_prophet = StructField("error_prophet", MapType(StringType(), FloatType()), nullable=False)
    _pred_prophet = StructField("pred_prophet", MapType(StringType(), ArrayType(FloatType(), containsNull=True)), nullable=False)
    _opt_param_prophet = StructField("prophet_params", MapType(StringType(), StringType()), nullable=False)

    schema = StructType([customernumber, mat_no, _error_prophet, _pred_prophet, _opt_param_prophet])

    return schema


def arima_output_schema():
    """
    Expected dataframe structure for the specifiec schema is
    (customernumber, mat_no, _error_arima(in dictionary format --> {string -> float}),
    _pred_arima(as a dictionary {string -> array(float)}), param(as dictionary {string -> string}))

    Example:::
    (u'0500083147', u'000000000000132218', {'period_days': 154, 'cum_quantity': 35.54545454545455, 'wre_max_6': 38.2428908091008, 'rmse': 0.63, 'wre_med_12': 12.635990296609734, 'mape': 40.69, 'mat_no': u'000000000000132218', 'wre_med_6': 6.86067324942004, 'cum_error': -0.8573985749159931, 'wre_max_12': 16.296254405296658, 'cus_no': u'0500083147'}, {u'yhat': [1.6055120386730186]}, {'yearly_seasonality': False, 'changepoint_prior_scale': 3, 'holidays': False})
    :return: schema of Row(customernumber, mat_no, error_prophet, pred_prophet, prophet_params)
    """

    customernumber = StructField("customernumber", StringType(), nullable=False)
    mat_no = StructField("mat_no", StringType(), nullable=False)
    _error_prophet = StructField("error_prophet", MapType(StringType(), FloatType()), nullable=False)
    _pred_prophet = StructField("pred_prophet", MapType(StringType(), ArrayType(FloatType(), containsNull=True)), nullable=False)
    _opt_param_prophet = StructField("prophet_params", MapType(StringType(), StringType()), nullable=False)

    schema = StructType([customernumber, mat_no, _error_prophet, _pred_prophet, _opt_param_prophet])

    return schema


a = ((u'0500064458', u'000000000000119826'), (11.475349130933282, (11.475349130933282, {'period_days': 196, 'cum_quantity': 96.81818181818181, 'wre_max_6': 20.0, 'rmse': 0.96, 'wre_med_12': -6.961470088105928, 'mape': 19.7, 'mat_no': u'000000000000119826', 'wre_med_6': -5.279081699973396, 'cum_error': -6.8917056285445115, 'wre_max_12': 11.475349130933282, 'cus_no': u'0500064458'}, [2.412280378066752], [1, 1, 0], [0, 0, 0, 52])))

customernumber, mat_no = a[0]
_error_arima = a[1][1][1]
_pred_arima = a[1][1][2]
pdq = a[1][1][3]
seasonal_pdq = a[1][1][4]

print customernumber
print _error_arima