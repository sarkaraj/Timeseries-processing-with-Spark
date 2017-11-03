from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from model.error_calculator_distributed_grid_search import monthly_prophet_model_error_calc
import transform_data.pandas_support_func as pd_func
from transform_data.data_transform import *
from properties import PROPH_M_MODEL_SELECTION_CRITERIA


def _get_pred_dict_prophet_m(prediction):
    prediction_df_temp = prediction.set_index('ds', drop=True)
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: x.strftime('%Y-%m-%d'))
    pred = prediction_df_temp.to_dict(orient='index')
    _final = {(int(key.split("-")[1]), int(key.split("-")[0])): float(pred.get(key).get('yhat'))
              for key in pred.keys()}
    return _final


def run_prophet_monthly(cus_no, mat_no, prod, param, **kwargs):
    import pandas as pd
    import numpy as np
    from dateutil import parser
    from fbprophet import Prophet
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
    if (param.get('yearly_seasonality') == True):
        yearly_seasonality = True
        seasonality_prior_scale = param.get('seasonality_prior_scale')
        changepoint_prior_scale = param.get('changepoint_prior_scale')

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
            # prophet
            m = Prophet(weekly_seasonality=False, yearly_seasonality=yearly_seasonality,
                        changepoint_prior_scale=changepoint_prior_scale,
                        seasonality_prior_scale=seasonality_prior_scale)
            m.fit(train);

            # creating pred train and test data frame
            # past = m.make_future_dataframe(periods=0, freq='W')
            future = pd.DataFrame(test['ds'])
            # pf_train_pred = m.predict(past)
            pf_test_pred = m.predict(future)
            # pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
            pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])

            # creating test and train emsembled result
            result_test = test
            result_test['y_Prophet'] = np.array(pf_test_pred.yhat)
            result_test.loc[(result_test['y_Prophet'] < 0), 'y_Prophet'] = 0

            train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
            test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
            # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

            output_result = pd.concat([output_result, result_test], axis=0)

        # Forecast
        m_ = Prophet(weekly_seasonality=False, yearly_seasonality=yearly_seasonality,
                     changepoint_prior_scale=changepoint_prior_scale,
                     seasonality_prior_scale=seasonality_prior_scale)
        m_.fit(prod);
        pred_ds = m_.make_future_dataframe(periods=pred_points, freq='M').tail(pred_points)
        pred_ds.ds = pred_ds.ds.map(lambda x: x + dt.timedelta(days=15))

        _prediction_temp = m_.predict(pred_ds)[['ds', 'yhat']]
        _prediction = _get_pred_dict_prophet_m(_prediction_temp)  # # get a dict {(month,year):pred_val}

        output_result = monthly_prophet_model_error_calc(output_result)
        # output_result_dict = output_result[['ds', 'y', 'y_Prophet']].to_dict(orient='index')

        output_error = pd.DataFrame(
            data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                   mape_calculator(output_result.y_Prophet, output_result.y),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_3month_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_3month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_3month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_3month_quantity))),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_4month_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_4month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_4month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_4month_quantity))),
                   output_result['Error_Cumsum_prophet'].iloc[-1],
                   output_result['cumsum_quantity'].iloc[-1],
                   ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 30)]],
            columns=['cus_no', 'mat_no', 'rmse', 'mape',
                     'mre_med_3', 'mre_max_3', 'mre_mean_3', 'quantity_mean_3',
                     'mre_med_4', 'mre_max_4', 'mre_mean_4', 'quantity_mean_4',
                     'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get(PROPH_M_MODEL_SELECTION_CRITERIA)
        _pdt_cat = kwargs.get('pdt_cat')
        # _result = ((cus_no, mat_no), (_criteria, output_error_dict, output_result_dict, _prediction, param))
        _result = ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))

        return _result

    elif (param.get('yearly_seasonality') == False):
        yearly_seasonality = False
        changepoint_prior_scale = param.get('changepoint_prior_scale')

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
            # prophet
            m = Prophet(weekly_seasonality=False, yearly_seasonality=yearly_seasonality,
                        changepoint_prior_scale=changepoint_prior_scale)
            m.fit(train);

            # creating pred train and test data frame
            # past = m.make_future_dataframe(periods=0, freq='W')
            future = pd.DataFrame(test['ds'])
            # pf_train_pred = m.predict(past)
            pf_test_pred = m.predict(future)
            # pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
            pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])

            # ceating test and train emsembled result
            result_test = test
            result_test['y_Prophet'] = np.array(pf_test_pred.yhat)
            result_test.loc[(result_test['y_Prophet'] < 0), 'y_Prophet'] = 0

            train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
            test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
            # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

            output_result = pd.concat([output_result, result_test], axis=0)

        # Forecast
        m_ = Prophet(weekly_seasonality=False, yearly_seasonality=yearly_seasonality,
                     changepoint_prior_scale=changepoint_prior_scale)
        m_.fit(prod);
        pred_ds = m_.make_future_dataframe(periods=pred_points, freq='M').tail(pred_points)
        pred_ds.ds = pred_ds.ds.map(lambda x: x + dt.timedelta(days=15))

        _prediction_temp = m_.predict(pred_ds)[['ds', 'yhat']]
        _prediction = _get_pred_dict_prophet_m(_prediction_temp)  # # get a dict {(month,year):pred_val}


        output_result = monthly_prophet_model_error_calc(output_result)
        # output_result_dict = output_result[['ds', 'y', 'y_Prophet']].to_dict(orient='index')

        output_error = pd.DataFrame(
            data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                   mape_calculator(output_result.y_Prophet, output_result.y),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_3month_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_3month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_3month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_3month_quantity))),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_4month_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_4month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_4month_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_4month_quantity))),
                   output_result['Error_Cumsum_prophet'].iloc[-1],
                   output_result['cumsum_quantity'].iloc[-1],
                   ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 30)]],
            columns=['cus_no', 'mat_no', 'rmse', 'mape',
                     'mre_med_3', 'mre_max_3', 'mre_mean_3', 'quantity_mean_3',
                     'mre_med_4', 'mre_max_4', 'mre_mean_4', 'quantity_mean_4',
                     'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get(PROPH_M_MODEL_SELECTION_CRITERIA)
        _pdt_cat = kwargs.get('pdt_cat')
        # _result = ((cus_no, mat_no), (_criteria, output_error_dict, output_result_dict, _prediction, param))
        _result = ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))

        return _result

    # except ValueError:
    #     return "MODEL_NOT_VALID"
    # except ZeroDivisionError:
    #     return "MODEL_NOT_VALID"
    # except RuntimeError:
    #     return "MODEL_NOT_VALID"
        # except AttributeError:
        #     return "MODEL_NOT_VALID"
        # except np.linalg.linalg.LinAlgError:
        #     return "MODEL_NOT_VALID"
