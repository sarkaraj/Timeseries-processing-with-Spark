from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from model.error_calculator_distributed_grid_search import weekly_prophet_error_calc
import transform_data.pandas_support_func as pd_func
from transform_data.holidays import get_holidays_dataframe_pd
from properties import PROPH_W_MODEL_SELECTION_CRITERIA
from transform_data.data_transform import gregorian_to_iso


def _get_pred_dict_prophet_w(prediction):
    """
    Get a dict {(weekNum,year):pred_val} from a pandas dataframe. weekNum --> Week Number of the year
    :param prediction: Pandas DataFrame:: DataFrame of the structure --> |date |prediction |
    :return: Dictionary:: {(weekNum,year): pred_val}
    """
    prediction_df_temp = prediction.set_index('ds', drop=True)
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: x.strftime('%Y-%m-%d'))
    pred = prediction_df_temp.to_dict(orient='index')
    _final = {
    (gregorian_to_iso(key.split("-"))[1], gregorian_to_iso(key.split("-"))[0]): float(pred.get(key).get('yhat'))
    for key in pred.keys()}
    return _final


def run_prophet(cus_no, mat_no, prod, param, **kwargs):
    """
    Execute Prophet given a specific set of parameters - WEEKLY run.
    :param cus_no: String:: Customer Number
    :param mat_no: String:: Material Number
    :param prod: Pandas DataFrame:: DataFrame containing the time series data
    :param param: Dictionary:: a dictionary containing the complete parameter set for a single Prophet instance
    :param kwargs:
                    1. 'min_train_days': Float/Int:: Minimum Training Days
                    2. 'test_points': Float/Int:: Number of test points for each successive cross-validation loop
                    3. 'pred_points': Float/Int:: Number of prediction points
                    4. 'pdt_cat': Dictionary:: All category specific information
    :return: Tuple:: Tuple of the structure
                    ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))

                    1. _criteria: Float/Double:: Selection criteria used to select the best parameter set for a given cust-pdt group
                    2. output_error_dict: Dictionary{String : Float}:: dictionary containing all the captured errors
                    3. _prediction: Dictionary{(String, String) : Float}:: dictionary containing the prediction values.
                                    Structure -->  {(weekNum,year): pred_val}
    """
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

        # Remove outlier
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True)

        # test and train data creation
        train = prod[
            prod.ds <= (
                np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
        output_result = pd.DataFrame()

        # # Forecast
        # m_ = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
        #              changepoint_prior_scale=changepoint_prior_scale,
        #              seasonality_prior_scale=seasonality_prior_scale)

        while (len(test) > 0):
            # prophet
            m_ = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
                         changepoint_prior_scale=changepoint_prior_scale,
                         seasonality_prior_scale=seasonality_prior_scale)
            m_.fit(train);

            # creating pred train and test data frame
            # past = m_.make_future_dataframe(periods=0, freq='W')
            future = pd.DataFrame(test['ds'])
            # pf_train_pred = m_.predict(past)
            pf_test_pred = m_.predict(future)
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
        pred_ds.ds = pred_ds.ds.map(lambda x: x + dt.timedelta(days=4))

        _prediction_temp = m_.predict(pred_ds)[['ds', 'yhat']]
        _prediction = _get_pred_dict_prophet_w(_prediction_temp)  # # get a dict {(weekNum,year):pred_val}
        # _prediction = m_.predict(pred_ds)[['ds', 'yhat']].to_dict(orient='list')

        output_result = weekly_prophet_error_calc(output_result)
        # output_result_dict = output_result[['ds', 'y', 'y_Prophet']].to_dict(orient='index')

        output_error = pd.DataFrame(
            data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                   mape_calculator(output_result.y_Prophet, output_result.y),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_6week_quantity))),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_12week_quantity))),
                   output_result['Error_Cumsum_prophet'].iloc[-1],
                   output_result['cumsum_quantity'].iloc[-1],
                   ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
            columns=['cus_no', 'mat_no', 'rmse', 'mape',
                     'wre_med_6', 'wre_max_6', 'wre_mean_6', 'quantity_mean_6',
                     'wre_med_12', 'wre_max_12', 'wre_mean_12', 'quantity_mean_12',
                     'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get(PROPH_W_MODEL_SELECTION_CRITERIA)
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

        # # Forecast
        # m_ = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
        #              changepoint_prior_scale=changepoint_prior_scale)

        while (len(test) > 0):
            # prophet
            m_ = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=yearly_seasonality,
                         changepoint_prior_scale=changepoint_prior_scale)
            m_.fit(train);

            # creating pred train and test data frame
            # past = m_.make_future_dataframe(periods=0, freq='W')
            future = pd.DataFrame(test['ds'])
            # pf_train_pred = m_.predict(past)
            pf_test_pred = m_.predict(future)
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
        pred_ds.ds = pred_ds.ds.map(lambda x: x + dt.timedelta(days=4))

        _prediction_temp = m_.predict(pred_ds)[['ds', 'yhat']]
        _prediction = _get_pred_dict_prophet_w(_prediction_temp)  # # get a dict {(weekNum,year):pred_val}

        output_result = weekly_prophet_error_calc(output_result)
        # output_result_dict = output_result[['ds', 'y', 'y_Prophet']].to_dict(orient='index')

        output_error = pd.DataFrame(
            data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                   mape_calculator(output_result.y_Prophet, output_result.y),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_6week_quantity))),
                   np.nanmedian(np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   np.nanmax(
                       np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
                   np.nanmean(np.absolute(np.array(output_result.rolling_12week_quantity))),
                   output_result['Error_Cumsum_prophet'].iloc[-1],
                   output_result['cumsum_quantity'].iloc[-1],
                   ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
            columns=['cus_no', 'mat_no', 'rmse', 'mape',
                     'wre_med_6', 'wre_max_6', 'wre_mean_6', 'quantity_mean_6',
                     'wre_med_12', 'wre_max_12', 'wre_mean_12', 'quantity_mean_12',
                     'cum_error', 'cum_quantity', 'period_days'])

        output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
        _criteria = output_error_dict.get(PROPH_W_MODEL_SELECTION_CRITERIA)
        _pdt_cat = kwargs.get('pdt_cat')
        _result = ((cus_no, mat_no), (_criteria, output_error_dict, _prediction, param, _pdt_cat))

        return _result

    # except ValueError:
    #     return "MODEL_NOT_VALID"
    # except ZeroDivisionError:
    #     return "MODEL_NOT_VALID"
        # except np.linalg.linalg.LinAlgError:
        #     return "MODEL_NOT_VALID"
