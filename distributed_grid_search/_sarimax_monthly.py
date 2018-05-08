from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from model.error_calculator_distributed_grid_search import monthly_arima_model_error_calc
import transform_data.pandas_support_func as pd_func
from transform_data.data_transform import gregorian_to_iso, string_to_gregorian
from transform_data.data_transform import get_monthly_aggregate_per_product
from distributed_grid_search.properties import SARIMAX_M_MODEL_SELECTION_CRITERIA


def _get_pred_dict_sarimax_m(prediction_series):
    import pandas as pd
    from dateutil import parser
    prediction_df_temp = prediction_series[1:].to_frame()
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: string_to_gregorian(str(x)+"-15"))
    print("prediction_df_temp.index 1")
    print(prediction_df_temp.index)
    # prediction_df_temp.index = prediction_df_temp.index.apply(parser.parse)
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: x.strftime('%Y-%m-%d'))
    print("prediction_df_temp.index 2")
    print(prediction_df_temp.index)

    pred = prediction_df_temp.to_dict(orient='index')
    print(pred)
    _final = {(gregorian_to_iso(key.split("-"))[1], gregorian_to_iso(key.split("-"))[0]): float(pred.get(key).get(0))
              for key in pred.keys()}
    print(_final)
    return _final


def sarimax_monthly(cus_no, mat_no, pdq, seasonal_pdq, prod, **kwargs):
    import pandas as pd
    import numpy as np
    from dateutil import parser
    import warnings
    import statsmodels.api as sm
    import datetime as dt

    if ('min_train_days' in kwargs.keys()):
        min_train_days = kwargs.get('min_train_days')
    else:
        min_train_days = p_model.min_train_days

    if ('test_points' in kwargs.keys()):
        test_points = kwargs.get('test_points')
    else:
        test_points = p_model.test_points_monthly

    if ('pred_points' in kwargs.keys()):
        pred_points = kwargs.get('pred_points')
    else:
        pred_points = p_model.pred_points_monthly

    # try:
    pdq = pdq
    seasonal_pdq = seasonal_pdq

    # data transform
    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    # Aggregated monthly data
    prod = get_monthly_aggregate_per_product(prod)

    # Remove outlier
    prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size=6, sigma=2.5)

    # test and train data creation
    train = prod[
        prod.ds <= (
            np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    output_result = pd.DataFrame()

    while (len(test) > 0):
        # ARIMA Model Data Transform
        train_arima = train.set_index('ds', drop=True)
        test_arima = test.set_index('ds', drop=True)

        train_arima.index = pd.period_range(start= min(train_arima.index),end= max(train_arima.index),freq= 'M')
        test_arima.index = pd.period_range(start=min(test_arima.index), end=max(test_arima.index), freq='M')
        print(train_arima.index)

        print(test_arima)

        warnings.filterwarnings("ignore")  # specify to ignore warning messages

        mod = sm.tsa.statespace.SARIMAX(train_arima, order=pdq, seasonal_order=seasonal_pdq,
                                        enforce_invertibility=False, enforce_stationarity=False,
                                        measurement_error=False, time_varying_regression=False,
                                        mle_regression=True)

        results = mod.fit(disp=False)

        ##########################################################################

        # forecast test
        pred_test = results.get_prediction(start=(np.amax(train_arima.index)),
                                           end=(np.amax(test_arima.index)), dynamic=True)

        # print(pred_test.predicted_mean)
        # pred_test_ci = pred_test.conf_int()

        # creating test and train ensembled result
        result_test = test
        result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]
        result_test.loc[(result_test['y_ARIMA'] < 0), 'y_ARIMA'] = 0

        train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

        output_result = pd.concat([output_result, result_test], axis=0)

    # model_prediction
    prod_arima = prod.set_index('ds', drop=True)
    prod_arima.index = pd.period_range(start=min(prod_arima.index), end=max(prod_arima.index), freq='M')
    mod = sm.tsa.statespace.SARIMAX(prod_arima, order=pdq, seasonal_order=seasonal_pdq,
                                    enforce_invertibility=False, enforce_stationarity=False,
                                    measurement_error=False, time_varying_regression=False,
                                    mle_regression=True)

    results_arima = mod.fit(disp=False)
    pred_arima = results_arima.get_prediction(start= max(prod_arima.index),
                                       end= max(prod_arima.index) + pred_points - 1, dynamic=True)

    # print(pred_arima.predicted_mean)
    _output_pred = _get_pred_dict_sarimax_m(pred_arima.predicted_mean)  # # get a dict {(weekNum,year):pred_val}

    print(_output_pred)
    output_result = monthly_arima_model_error_calc(output_result)
    # output_result_dict = output_result[['ds','y','y_ARIMA']].to_dict(orient='index')

    output_error = pd.DataFrame(
        data=[[cus_no, mat_no, rmse_calculator(output_result.y_ARIMA, output_result.y),
               mape_calculator(output_result.y_ARIMA, output_result.y),
               np.nanmedian(np.absolute(np.array(output_result.rolling_3month_percent_error_arima))),
               np.nanmax(
                   np.absolute(np.array(output_result.rolling_3month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_3month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_3month_quantity))),
               np.nanmedian(np.absolute(np.array(output_result.rolling_4month_percent_error_arima))),
               np.nanmax(
                   np.absolute(np.array(output_result.rolling_4month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_4month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_4month_quantity))),
               output_result['Error_Cumsum_arima'].iloc[-1],
               output_result['cumsum_quantity'].iloc[-1],
               ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 30)]],
        columns=['cus_no', 'mat_no', 'rmse', 'mape',
                 'mre_med_3', 'mre_max_3', 'mre_mean_3', 'quantity_mean_3',
                 'mre_med_4', 'mre_max_4', 'mre_mean_4', 'quantity_mean_4',
                 'cum_error', 'cum_quantity', 'period_days'])

    output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
    _criteria = output_error_dict.get(SARIMAX_M_MODEL_SELECTION_CRITERIA)
    pdt_category = kwargs.get('pdt_cat')
    _result = (
    (cus_no, mat_no), (_criteria, output_error_dict, _output_pred, list(pdq), list(seasonal_pdq), pdt_category))
    # _result = ((cus_no, mat_no), (_criteria, output_error_dict, output_result_dict, pdq, seasonal_pdq))

    return _result

    # except ValueError:
    #     return "MODEL_NOT_VALID"
    # except ZeroDivisionError:
    #     return "MODEL_NOT_VALID"
    # except np.linalg.linalg.LinAlgError:
    #     return "MODEL_NOT_VALID"
    # except IndexError:
    #     return "MODEL_NOT_VALID"