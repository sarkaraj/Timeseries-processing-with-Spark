from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from model.error_calculator_distributed_grid_search import weekly_prophet_error_calc
import transform_data.pandas_support_func as pd_func
from transform_data.holidays import get_holidays_dataframe_pd


def run_prophet(cus_no, mat_no, prod, param, **kwargs):
    import pandas as pd
    import numpy as np
    from dateutil import parser
    from fbprophet import Prophet

    if ('min_train_days' in kwargs.keys()):
        min_train_days = kwargs.get('min_train_days')
    else:
        min_train_days = p_model.min_train_days

    if ('test_points' in kwargs.keys()):
        test_points = kwargs.get('test_points')
    else:
        test_points = p_model.test_points

    if ('pred_points' in kwargs.keys()):
        pred_points = kwargs.get('pred_points')
    else:
        pred_points = p_model.pred_points

    # try:
    if (param.get('yearly_seasonality') == True):
        yearly_seasonality = True
        seasonality_prior_scale = param.get('seasonality_prior_scale')
        changepoint_prior_scale = param.get('changepoint_prior_scale')
        if (param.get('holidays') == True):
            holidays = get_holidays_dataframe_pd()
        else:
            holidays = None

        # data transform
        prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
        prod = prod[['ds', 'y']]
        prod.ds = prod.ds.apply(str).apply(parser.parse)
        prod.y = prod.y.apply(float)
        prod = prod.sort_values('ds')
        prod = prod.reset_index(drop=True)
        # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

        # Remove outlier
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True)

        # test and train data creation
        train = prod[
            prod.ds <= (
                np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
        output_result = pd.DataFrame()

        while (len(test) > 0):
            # prophet
            m = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
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

            # ceating test and train emsembled result
            result_test = test
            result_test['y_Prophet'] = np.array(pf_test_pred.yhat)
            result_test.loc[(result_test['y_Prophet'] < 0), 'y_Prophet'] = 0

            train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
            test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
            # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

            output_result = pd.concat([output_result, result_test], axis=0)

        # Forecast
        m_ = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
                     changepoint_prior_scale=changepoint_prior_scale,
                     seasonality_prior_scale=seasonality_prior_scale)
        m_.fit(prod);
        pred_ds = m_.make_future_dataframe(periods=pred_points, freq='W').tail(pred_points)
        _prediction = m_.predict(pred_ds)[['yhat']].to_dict(orient='list')

        output_result = weekly_prophet_error_calc(output_result)
        # output_result_dict = output_result[['ds', 'y', 'y_Prophet']].to_dict(orient='index')

        output_error = pd.DataFrame(
            data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                   mape_calculator(output_result.y_Prophet, output_result.y),
                   np.nanmedian(output_result.rolling_6week_percent_error_prophet),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmedian(output_result.rolling_12week_percent_error_prophet),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   output_result['Error_Cumsum_prophet'].iloc[-1],
                   output_result['cumsum_quantity'].iloc[-1],
                   ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
            columns=['cus_no', 'mat_no', 'rmse', 'mape', 'wre_med_6', 'wre_max_6',
                     'wre_med_12', 'wre_max_12', 'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get('wre_max_12')
        _pdt_cat = kwargs.get('pdt_cat')
        _result = ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))

        return _result

    elif (param.get('yearly_seasonality') == False):
        yearly_seasonality = False
        changepoint_prior_scale = param.get('changepoint_prior_scale')
        if (param.get('holidays') == True):
            holidays = get_holidays_dataframe_pd()
        else:
            holidays = None

        # data transform
        prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
        prod = prod[['ds', 'y']]
        prod.ds = prod.ds.apply(str).apply(parser.parse)
        prod.y = prod.y.apply(float)
        prod = prod.sort_values('ds')
        prod = prod.reset_index(drop=True)
        # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

        # Remove outlier
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True)

        # test and train data creation
        train = prod[
            prod.ds <= (
                np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
        output_result = pd.DataFrame()

        while (len(test) > 0):
            # prophet
            m = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
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
        m_ = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
                     changepoint_prior_scale=changepoint_prior_scale)
        m_.fit(prod);
        pred_ds = m_.make_future_dataframe(periods=pred_points, freq='W').tail(pred_points)
        _prediction = m_.predict(pred_ds)[['yhat']].to_dict(orient='list')

        output_result = weekly_prophet_error_calc(output_result)
        # output_result_dict = output_result[['ds', 'y', 'y_Prophet']].to_dict(orient='index')

        output_error = pd.DataFrame(
            data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                   mape_calculator(output_result.y_Prophet, output_result.y),
                   np.nanmedian(output_result.rolling_6week_percent_error_prophet),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmedian(output_result.rolling_12week_percent_error_prophet),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   output_result['Error_Cumsum_prophet'].iloc[-1],
                   output_result['cumsum_quantity'].iloc[-1],
                   ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
            columns=['cus_no', 'mat_no', 'rmse', 'mape', 'wre_med_6', 'wre_max_6',
                     'wre_med_12', 'wre_max_12', 'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get('wre_max_12')
        _pdt_cat = kwargs.get('pdt_cat')
        _result = ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))

        return _result

    # except ValueError:
    #     return "MODEL_NOT_VALID"
    # except ZeroDivisionError:
    #     return "MODEL_NOT_VALID"
        # except np.linalg.linalg.LinAlgError:
        #     return "MODEL_NOT_VALID"
