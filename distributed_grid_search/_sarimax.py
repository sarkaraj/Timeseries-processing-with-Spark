from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from model.error_calculator_distributed_grid_search import weekly_arima_error_calc
import transform_data.pandas_support_func as pd_func


def sarimax(cus_no, mat_no, pdq, seasonal_pdq, prod, **kwargs):
    import pandas as pd
    import numpy as np
    import warnings
    import statsmodels.api as sm
    from dateutil import parser

    if(kwargs.has_key('min_train_days')):
        min_train_days=kwargs.get('min_train_days')
    else:
        min_train_days = p_model.min_train_days

    if(kwargs.has_key('test_points')):
        test_points=kwargs.get('test_points')
    else:
        test_points = p_model.test_points

    try:
        pdq = pdq
        seasonal_pdq = seasonal_pdq

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

        value_error_counter = 0

        while (len(rem_data.ds) >= test_points):
            # ARIMA Model Data Transform
            train_arima = train.set_index('ds', drop=True)
            test_arima = test.set_index('ds', drop=True)

            warnings.filterwarnings("ignore")  # specify to ignore warning messages

            mod = sm.tsa.statespace.SARIMAX(train_arima, order=pdq, seasonal_order=seasonal_pdq,
                                            enforce_stationarity=True, enforce_invertibility=True,
                                            measurement_error=False, time_varying_regression=False,
                                            mle_regression=True)

            results = mod.fit(disp=False)

            ##########################################################################

            # forecast test
            pred_test = results.get_prediction(start=pd.to_datetime(np.amax(train_arima.index)),
                                               end=pd.to_datetime(np.amax(test_arima.index)), dynamic=True)

            # pred_test_ci = pred_test.conf_int()

            # creating test and train ensembled result
            result_test = test
            result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]
            result_test.loc[(result_test['y_ARIMA'] < 0), 'y_ARIMA'] = 0

            train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
            test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
            rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

            output_result = pd.concat([output_result, result_test], axis=0)


        output_result = weekly_arima_error_calc(output_result)
        output_result_dict = output_result[['ds','y','y_ARIMA']].to_dict(orient='index')

        output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse_calculator(output_result.y_ARIMA, output_result.y),
                                           mape_calculator(output_result.y_ARIMA, output_result.y),
                                           np.nanmedian(output_result.rolling_6week_percent_error_arima),
                                           np.nanmax(
                                               np.absolute(np.array(output_result.rolling_6week_percent_error_arima))),
                                           np.nanmedian(output_result.rolling_12week_percent_error_arima),
                                           np.nanmax(
                                               np.absolute(np.array(output_result.rolling_12week_percent_error_arima))),
                                           output_result['Error_Cumsum_arima'].iloc[-1],
                                           output_result['cumsum_quantity'].iloc[-1],
                                           ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
                                    columns=['cus_no', 'mat_no', 'rmse', 'mape', '6wre_med', '6wre_max',
                                             '12wre_med', '12wre_max', 'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get('12wre_max')
        _result = ((cus_no, mat_no), (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq, value_error_counter))

        return _result

    except:
        return "MODEL_NOT_VALID"

