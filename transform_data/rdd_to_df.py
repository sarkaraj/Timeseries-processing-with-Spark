from pyspark.sql.types import *

# # _________________PROPHET__________________________ # #

def map_for_output_prophet(line):
    customernumber, mat_no = line[0]
    _error_prophet = line[1][1][1]
    _req_error_prophet_param = {key: float(_error_prophet.get(key)) for key in _error_prophet.keys() if key not in ('mat_no', 'cus_no')}
    _pred_prophet = line[1][1][2]
    _opt_param = line[1][1][3]
    _opt_param_prophet = {key:str(_opt_param.get(key)) for key in _opt_param.keys()}
    _pdt_cat = line[1][1][4]

    _result = customernumber, mat_no, _req_error_prophet_param, _pred_prophet, _opt_param_prophet, _pdt_cat
    return _result


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
    _pdt_category = StructField("pdt_cat", MapType(StringType(), StringType()), nullable=False)

    schema = StructType([customernumber, mat_no, _error_prophet, _pred_prophet, _opt_param_prophet, _pdt_category])

    return schema


# # _________________ARIMA__________________________ # #

def arima_output_schema():
    """
    Expected dataframe structure for the specifiec schema is
    (customernumber, mat_no, _error_arima(in dictionary format --> {string -> float}),
    _pred_arima(as a dictionary {string -> array(float)}), param(as dictionary {string -> string}))

    Example:::
    (u'0500064458', u'000000000000119826', {'period_days': 196.0, 'cum_error': -6.8917056285445115, 'wre_max_6': 20.0, 'rmse': 0.96, 'wre_med_12': -6.961470088105928, 'mape': 19.7, 'wre_med_6': -5.279081699973396, 'cum_quantity': 96.81818181818181, 'wre_max_12': 11.475349130933282}, {'yhat': [2.412280378066752]}, {'seasonal_pdq': [0, 0, 0, 52], 'pdq': [1, 1, 0]})
    :return: schema of Row(customernumber, mat_no, error_prophet, pred_prophet, prophet_params)
    """

    customernumber = StructField("customernumber", StringType(), nullable=False)
    mat_no = StructField("mat_no", StringType(), nullable=False)
    _error_arima = StructField("error_arima", MapType(StringType(), FloatType()), nullable=False)
    _pred_arima = StructField("pred_arima", MapType(StringType(), ArrayType(FloatType(), containsNull=True)), nullable=False)
    _opt_param_arima = StructField("arima_params", MapType(StringType(), ArrayType(IntegerType())), nullable=False)
    _pdt_category = StructField("pdt_cat", MapType(StringType(), StringType()), nullable=False)

    schema = StructType([customernumber, mat_no, _error_arima, _pred_arima, _opt_param_arima, _pdt_category])

    return schema


def map_for_output_arima(line):
    customernumber, mat_no = line[0]

    _error_arima = line[1][1][1]

    _req_error_arima_param = {key: float(_error_arima.get(key)) for key in _error_arima.keys() if key not in ('mat_no', 'cus_no')}

    _pred_arima = {'yhat':line[1][1][2]}

    pdq = line[1][1][3]
    seasonal_pdq = line[1][1][4]
    _pdt_cat = line[1][1][5]
    _opt_param_arima = {'pdq':pdq, 'seasonal_pdq':seasonal_pdq}

    _result = customernumber, mat_no, _req_error_arima_param, _pred_arima, _opt_param_arima, _pdt_cat

    return _result


def MA_output_schema():
    customernumber = StructField("customernumber", StringType(), nullable=False)
    mat_no = StructField("mat_no", StringType(), nullable=False)
    _error_ma = StructField("error_MA", MapType(StringType(), FloatType()), nullable=False)
    _pred_ma = StructField("pred_MA", MapType(StringType(), ArrayType(FloatType(), containsNull=True)),
                           nullable=False)
    _pdt_category = StructField("pdt_cat", MapType(StringType(), StringType()), nullable=False)

    schema = StructType([customernumber, mat_no, _error_ma, _pred_ma, _pdt_category])

    return schema


def map_for_output_MA_monthly(line):
    customernumber = line[0]
    mat_no = line[1]
    _error_ma = {key: float(line[2].get(key)) for key in line[2].keys() if key not in ('mat_no', 'cus_no')}
    _pred_ma = {'yhat': list(line[3])}
    _pdt_cat = line[4]

    _result = customernumber, mat_no, _error_ma, _pred_ma, _pdt_cat
    return _result


def map_for_output_MA_weekly(line):
    customernumber = line[0]
    mat_no = line[1]
    _error_ma = {key: float(line[2].get(key)) for key in line[2].keys() if key not in ('mat_no', 'cus_no')}
    _pred_ma = {'yhat': list(line[3])}
    _pdt_cat = line[4]

    _result = customernumber, mat_no, _error_ma, _pred_ma, _pdt_cat
    return _result
