from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from model.error_calculator_distributed_grid_search import monthly_arima_model_error_calc
import transform_data.pandas_support_func as pd_func
from transform_data.data_transform import gregorian_to_iso, string_to_gregorian
from transform_data.data_transform import get_monthly_aggregate_per_product
from distributed_grid_search.properties import SARIMAX_M_MODEL_SELECTION_CRITERIA
from model.save_images import *

def _get_pred_dict_sarimax_m(prediction_series):
    import pandas as pd
    from dateutil import parser

    prediction_df_temp = prediction_series[1:].to_frame()
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: string_to_gregorian(str(x)+"-15"))
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: x.strftime('%Y-%m-%d'))

    # pred is of the form {'2018-04-15': {0: 0.723894290308531}}
    pred = prediction_df_temp.to_dict(orient='index')
    _final = {(int(key.split("-")[1]), int(key.split("-")[0])): float(pred.get(key).get(0))
              for key in pred.keys()}
    return _final


def sarimax_monthly(cus_no, mat_no, pdq, seasonal_pdq, trend, prod, run_locally = False, **kwargs):
    """
    function fits sarimax model on the monthly data(cat IV, V and VI) for the given parameter set,
    performs CV, calculates CV error and makes future prediction.
    :param cus_no: customer number
    :param mat_no: material number
    :param pdq: pdq parameter tuple(e.g. (1,0,1))
    :param seasonal_pdq: seasonal pdq(e.g. (1,1,0,12)
    :param prod: time series data frame for a material for the given customer
        (structure:- ds: date(datetime), y: quantity(float))
    :param kwargs:
        :min_train_days: minimum training period for the CV to start for the remain test data
        :test_points: number of points ahead to make prediction for the each CV step
        :pred_points: future prediction points
        :pdt_cat: Product category object
    :return: ((cus_no, mat_no),
    (_criteria, output_error_dict, _output_pred, list(pdq), list(seasonal_pdq), pdt_category))
    """
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

    if ('image_dir' in kwargs.keys()):
        image_dir = kwargs.get('image_dir')

    # try:
    pdq = pdq
    seasonal_pdq = seasonal_pdq
    trend = trend

    ###############################################################
    # # Uncomment this part of the code to run it locally
    ###############################################################
    # # data transform
    # prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    # prod = prod[['ds', 'y']]
    # prod.ds = prod.ds.apply(str).apply(parser.parse)
    # prod.y = prod.y.apply(float)
    # prod = prod.sort_values('ds')
    # prod = prod.reset_index(drop=True)
    # # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)
    # # Remove outlier
    # prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size=6, sigma=2.5)
    ################################################################

    ################################################################
    # First split of test and train data
    ################################################################
    train = prod[
        prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days -
                                                          min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    ##################################################################

    #################################################################
    # Cross validation step: looping through all the test data points step by step through redefining the
    # train and test set iteratively
    #################################################################
    output_result = pd.DataFrame()  # Data frame to store actual and predicted quantities for cross validation set
    fit_counter = 0
    while (len(test) > 0):
        # Changing the index to date column to make it model consumable
        train_arima = train.set_index('ds', drop=True)
        test_arima = test.set_index('ds', drop=True)

        ##########################################################################
        # Model fitting
        ##########################################################################
        train_arima.index = pd.period_range(start= min(train_arima.index),end= max(train_arima.index),freq= 'M')
        test_arima.index = pd.period_range(start=min(test_arima.index), end=max(test_arima.index), freq='M')

        warnings.filterwarnings("ignore")  # specify to ignore warning messages

        mod = sm.tsa.statespace.SARIMAX(train_arima, order=pdq, seasonal_order=seasonal_pdq, trend = trend,
                                        enforce_invertibility=False, enforce_stationarity=False,
                                        measurement_error=False, time_varying_regression=False,
                                        mle_regression=True)

        results = mod.fit(disp=False)
        ##########################################################################

        ##########################################################################
        # generating forecast for test data points
        ##########################################################################
        pred_test = results.get_prediction(start=(np.amax(train_arima.index)),
                                           end=(np.amax(test_arima.index)), dynamic=True)
        result_test = test
        result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]
        result_test.loc[(result_test['y_ARIMA'] < 0), 'y_ARIMA'] = 0
        ##########################################################################

        ##########################################################################
        # save plots for CV fit and predictions at each CV step
        ##########################################################################
        if run_locally == True:
            pred_train = results.get_prediction(start=np.amin(np.array(train_arima.index)), dynamic=False)
            result_train = train
            result_train['y_ARIMA'] = np.array(pred_train.predicted_mean)
            three_dim_save_plot(x1=prod.ds, y1=prod.y, y1_label="Actual",
                                x2=result_test.ds, y2=result_test.y_ARIMA, y2_label='Predicted',
                                x3=result_train.ds, y3=result_train.y_ARIMA, y3_label='Model_fit',
                                xlable="Date", ylable="Quantity",
                                text= "pdq:" + str(pdq) + " pdq_seasonal:" + str(seasonal_pdq),
                                title="CV_fit_" + str(fit_counter),
                                dir_name=image_dir, cus_no=cus_no, mat_no=mat_no)
        ##########################################################################

        ##########################################################################
        # recreating test and train data set for next step of CV
        ##########################################################################
        train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
        ##########################################################################

        # appending the cross validation results at each step
        output_result = pd.concat([output_result, result_test], axis=0)
        fit_counter += 1

    ##############################################################################
    # save plot for complete CV predictions
    ##############################################################################
    if run_locally == True:
        data_baseline = prod.copy()
        data_baseline['rolling_mean'] = pd.rolling_mean(data_baseline['y'].shift(), window=3, min_periods=1)
        baseline_res = data_baseline.loc[-len(output_result):]
        baseline_res = baseline_res.reset_index(drop=True)
        three_dim_save_plot(x1=prod.ds, y1=prod.y, y1_label="Actual",
                            x2=output_result["ds"], y2=output_result["y_ARIMA"], y2_label="Predicted",
                            x3=baseline_res.ds, y3= baseline_res.rolling_mean, y3_label="Baseline", y3_color= 'purple',
                            xlable= "Date", ylable= "Quantity",
                            text="pdq:" + str(pdq) + " pdq_seasonal:" + str(seasonal_pdq),
                            title="Baseline_vs_ARIMA_Prediction",
                            dir_name=image_dir, cus_no=cus_no, mat_no=mat_no)
    ##############################################################################

    ##############################################################################
    # Model building on complete data set to generate out of sample prediction
    ##############################################################################
    prod_arima = prod.set_index('ds', drop=True)
    prod_arima.index = pd.period_range(start=min(prod_arima.index), end=max(prod_arima.index), freq='M')
    mod = sm.tsa.statespace.SARIMAX(prod_arima, order=pdq, seasonal_order=seasonal_pdq, trend = trend,
                                    enforce_invertibility=False, enforce_stationarity=False,
                                    measurement_error=False, time_varying_regression=False,
                                    mle_regression=True)

    results_arima = mod.fit(disp=False)
    pred_arima = results_arima.get_prediction(start= max(prod_arima.index),
                                       end= max(prod_arima.index) + pred_points, dynamic=True)
    # making out of sample predictions
    pred_out_sample = pred_arima.predicted_mean
    pred_out_sample[pred_out_sample <0] = 0
    _output_pred = _get_pred_dict_sarimax_m(pred_out_sample)  # # get a dict {(weekNum,year):pred_val}
    ###############################################################################

    ###############################################################################
    # Error calculation
    ###############################################################################
    output_result = monthly_arima_model_error_calc(output_result)
    output_error = pd.DataFrame(
        data=[[cus_no, mat_no, rmse_calculator(output_result.y_ARIMA, output_result.y),
               mape_calculator(output_result.y_ARIMA, output_result.y),
               np.nanmedian(np.absolute(np.array(output_result.rolling_3month_percent_error_arima))),
               np.nanmax(np.absolute(np.array(output_result.rolling_3month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_3month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_3month_quantity))),
               np.nanmedian(np.absolute(np.array(output_result.rolling_4month_percent_error_arima))),
               np.nanmax(np.absolute(np.array(output_result.rolling_4month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_4month_percent_error_arima))),
               np.nanmean(np.absolute(np.array(output_result.rolling_4month_quantity))),
               output_result['Error_Cumsum_arima'].iloc[-1],
               output_result['cumsum_quantity'].iloc[-1],
               ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 30)]],
        columns=['cus_no', 'mat_no', 'rmse', 'mape',
                 'mre_med_3', 'mre_max_3', 'mre_mean_3', 'quantity_mean_3',
                 'mre_med_4', 'mre_max_4', 'mre_mean_4', 'quantity_mean_4',
                 'cum_error', 'cum_quantity', 'period_days'])
    ##############################################################################

    ##############################################################################
    # Output Preparation
    ##############################################################################
    output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
    _criteria = output_error_dict.get(SARIMAX_M_MODEL_SELECTION_CRITERIA)
    pdt_category = kwargs.get('pdt_cat')
    _result = (
    (cus_no, mat_no), (_criteria, output_error_dict, _output_pred, list(pdq), list(seasonal_pdq),list(trend), pdt_category))
    ##############################################################################

    return _result

    # except ValueError:
    #     return "MODEL_NOT_VALID"
    # except ZeroDivisionError:
    #     return "MODEL_NOT_VALID"
    # except np.linalg.linalg.LinAlgError:
    #     return "MODEL_NOT_VALID"
    # except IndexError:
    #     return "MODEL_NOT_VALID"