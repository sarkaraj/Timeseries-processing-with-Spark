from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
from transform_data.holidays import get_holidays_dataframe_pd
import distributed_grid_search.properties as p_model
from model.ma_outlier import ma_replace_outlier


def sarimax(cus_no, mat_no, prod, param, **kwargs):
    import pandas as pd
    import numpy as np
    import itertools
    import warnings
    import statsmodels.api as sm
    from fbprophet import Prophet
    from dateutil import parser

    if(kwargs.has_key('min_train_days')):
        min_train_days=kwargs.get('min_train_days')
    else:
        min_train_days = p_model.min_train_days

    if(kwargs.has_key('test_points')):
        test_points=kwargs.get('test_points')
    else:
        test_points = p_model.test_points

    pdq, seasonal_pdq = param

    # data transform
    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    # Remove outlier
    prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True)

    # test and train data creation
    train = prod[
        prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    output_result = pd.DataFrame()

    while (len(rem_data.ds) >= test_points):
        # ARIMA Model Data Transform
        train_arima = train.set_index('ds', drop=True)
        test_arima = test.set_index('ds', drop=True)

        warnings.filterwarnings("ignore")  # specify to ignore warning messages

        mod = sm.tsa.statespace.SARIMAX(train_arima, order=pdq, seasonal_order=seasonal_pdq,
                                        enforce_stationarity=True, enforce_invertibility=True,
                                        measurement_error=False, time_varying_regression=False,
                                        mle_regression=True)

        result = mod.fit(disp=False)

        # forecast Train
        pred_train = results.get_prediction(start=pd.to_datetime(np.amin(np.array(train_arima.index))), dynamic=False)
        # pred_train_ci = pred_train.conf_int()

        # forecast test
        pred_test = results.get_prediction(start=pd.to_datetime(np.amax(train_arima.index)),
                                           end=pd.to_datetime(np.amax(test_arima.index)), dynamic=True)
        # pred_test_ci = pred_test.conf_int()

        # creating test and train ensembled result
        result_test = test
        result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]
        result_test.loc[(result_test['y_ARIMA'] < 0), 'y_ARIMA'] = 0

        #TODO: think of what to capture from the arima fit --> this will be the criteria for model selection

        train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

    output_result = weekly_ensm_model_error_calc(output_result)

    output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                                       mape_calculator(output_result.y_Prophet, output_result.y),
                                       np.nanmedian(output_result.rolling_6week_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_6week_percent_error))),
                                       np.nanmedian(output_result.rolling_6week_percent_error_prophet),
                                       np.nanmax(
                                           np.absolute(
                                               np.array(output_result.rolling_6week_percent_error_prophet))),
                                       np.nanmedian(output_result.rolling_6week_percent_error_arima),
                                       np.nanmax(
                                           np.absolute(np.array(output_result.rolling_6week_percent_error_arima))),
                                       np.nanmedian(output_result.rolling_12week_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_12week_percent_error))),
                                       np.nanmedian(output_result.rolling_12week_percent_error_prophet),
                                       np.nanmax(
                                           np.absolute(
                                               np.array(output_result.rolling_12week_percent_error_prophet))),
                                       np.nanmedian(output_result.rolling_12week_percent_error_arima),
                                       np.nanmax(
                                           np.absolute(np.array(output_result.rolling_12week_percent_error_arima))),
                                       output_result['Error_Cumsum'].iloc[-1],
                                       output_result['cumsum_quantity'].iloc[-1],
                                       ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
                                columns=['cus_no', 'mat_no', 'rmse', 'mape', '6wre_med', '6wre_max',
                                         '6wre_med_prophet',
                                         '6wre_max_prophet', '6wre_med_arima', '6wre_max_arima',
                                         '12wre_med', '12wre_max', '12wre_med_prophet', '12wre_max_prophet',
                                         '12wre_med_arima', '12wre_max_arima',
                                         'cum_error', 'cum_quantity', 'period_days'])

    return output_error

