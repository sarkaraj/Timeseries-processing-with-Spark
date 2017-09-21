from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
from transform_data.holidays import get_holidays_dataframe_pd


def weekly_ensm_model(prod, cus_no, mat_no, min_train_days=731, test_points=2, holidays=get_holidays_dataframe_pd(),
                      **kwargs):
    """
        Prod: weekly aggregated data frame containing
            columns: customernumber, matnr, dt_week, quantity, p_ind_quantity
        dir_name: where to save files
        holidays: DataFrame containing list of holidays as required by prophet
        Define dir_name to save images

    """
    import pandas as pd
    import numpy as np
    import itertools
    import warnings
    import statsmodels.api as sm
    from fbprophet import Prophet
    from dateutil import parser
    import datetime as dt

    # data transform
    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    # save plot (comment)
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="raw_weekly_aggregated_quantity",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # Remove outlier
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True
                                  , dir_name=dir_name, mat_no=mat_no, cus_no=cus_no)
    else:
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True)

    # save plot (comment)
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="weekly_aggregated_quantity_outlier_replaced",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # # test and train data creation
    # train = prod[
    #     prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    # test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    # output_result = pd.DataFrame()
    #
    # # incremental test
    # while (len(rem_data.ds) >= test_points):
    #
    #     # ARIMA Model Data Transform
    #     train_arima = train.set_index('ds', drop=True)
    #     test_arima = test.set_index('ds', drop=True)
    #
    #     # ARIMA Model
    #     # grid search parameters
    #     p = [0, 1]
    #     d = [0, 1]
    #     q = [0, 1]
    #
    #     # Generate all different combinations of p, q and q triplets
    #     pdq = list(itertools.product(p, d, q))
    #
    #     # Generate all different combinations of seasonal p, q and q triplets
    #     seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(p, d, q))]
    #
    #     # grid search
    #     warnings.filterwarnings("ignore")  # specify to ignore warning messages
    #     value_error_count = 0
    #     min_aic = 9999999
    #     opt_param = (0, 0, 0)
    #     opt_param_seasonal = (0 , 0, 0, 52)
    #     for param in pdq:
    #         for param_seasonal in seasonal_pdq:
    #             try:
    #                 mod = sm.tsa.statespace.SARIMAX(train_arima, order= param, seasonal_order= param_seasonal,
    #                                                 enforce_stationarity=True, enforce_invertibility=True,
    #                                                 measurement_error=False, time_varying_regression=False,
    #                                                 mle_regression=True)
    #
    #                 results = mod.fit(disp=False)
    #                 if results.aic < min_aic:
    #                     min_aic = results.aic
    #                     opt_param = param
    #                     opt_param_seasonal = param_seasonal
    #
    #                     # print('ARIMA{}x{}52 - AIC:{}'.format(param, param_seasonal, results.aic))
    #             except:
    #                 value_error_count = value_error_count +1
    #                 continue
    #
    #     print('Optimal ARIMA{}x{}52 - AIC:{}'.format(opt_param, opt_param_seasonal, min_aic))
    #     print('value error{}'.format(value_error_count))
    #     # fitting Model
    #     mod = sm.tsa.statespace.SARIMAX(train_arima, order= opt_param, seasonal_order= opt_param_seasonal,
    #                                     enforce_stationarity=True, enforce_invertibility=True,
    #                                     measurement_error=False, time_varying_regression=False,
    #                                     mle_regression=True)
    #     results = mod.fit(disp=False)
    #
    #     # forecast Train
    #     pred_train = results.get_prediction(start=pd.to_datetime(np.amin(np.array(train_arima.index))), dynamic=False)
    #     # pred_train_ci = pred_train.conf_int()
    #
    #     # forecast test
    #     pred_test = results.get_prediction(start=pd.to_datetime(np.amax(train_arima.index)),
    #                                        end=pd.to_datetime(np.amax(test_arima.index)), dynamic=True)
    #     # pred_test_ci = pred_test.conf_int()
    #
    #     # creating test and train ensembled result
    #     result_test = test
    #     result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]
    #     result_test.loc[(result_test['y_ARIMA'] < 0), 'y_ARIMA'] = 0
    #
    #     # prophet
    #     m = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=True, changepoint_prior_scale=5)
    #     m.fit(train);
    #
    #     # creating pred train and test data frame
    #     past = m.make_future_dataframe(periods=0, freq='W')
    #     future = pd.DataFrame(test['ds'])
    #     pf_train_pred = m.predict(past)
    #     pf_test_pred = m.predict(future)
    #     pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
    #     pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])
    #
    #     # ceating test and train emsembled result
    #     result_test['y_Prophet'] = np.array(pf_test_pred.yhat)
    #     result_test.loc[(result_test['y_Prophet'] < 0), 'y_Prophet'] = 0
    #
    #     # Ansemble
    #     result_test['y_Ensembled'] = result_test[["y_ARIMA", "y_Prophet"]].mean(axis=1)
    #
    #     print('Next Test Starts...')
    #     train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
    #     test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    #     rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    #
    #     output_result = pd.concat([output_result, result_test], axis=0)
    #
    # output_result = weekly_ensm_model_error_calc(output_result)
    #
    # output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
    #                                    mape_calculator(output_result.y_Prophet, output_result.y),
    #                                    np.nanmedian(output_result.rolling_6week_percent_error),
    #                                    np.nanmax(np.absolute(np.array(output_result.rolling_6week_percent_error))),
    #                                    np.nanmedian(output_result.rolling_6week_percent_error_prophet),
    #                                    np.nanmax(
    #                                        np.absolute(np.array(output_result.rolling_6week_percent_error_prophet))),
    #                                    np.nanmedian(output_result.rolling_6week_percent_error_arima),
    #                                    np.nanmax(
    #                                        np.absolute(np.array(output_result.rolling_6week_percent_error_arima))),
    #                                    np.nanmedian(output_result.rolling_12week_percent_error),
    #                                    np.nanmax(np.absolute(np.array(output_result.rolling_12week_percent_error))),
    #                                    np.nanmedian(output_result.rolling_12week_percent_error_prophet),
    #                                    np.nanmax(
    #                                        np.absolute(np.array(output_result.rolling_12week_percent_error_prophet))),
    #                                    np.nanmedian(output_result.rolling_12week_percent_error_arima),
    #                                    np.nanmax(
    #                                        np.absolute(np.array(output_result.rolling_12week_percent_error_arima))),
    #                                    output_result['Error_Cumsum'].iloc[-1],
    #                                    output_result['cumsum_quantity'].iloc[-1],
    #                                    ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
    #                             columns=['cus_no', 'mat_no', 'rmse', 'mape', '6wre_med', '6wre_max', '6wre_med_prophet',
    #                                      '6wre_max_prophet', '6wre_med_arima', '6wre_max_arima',
    #                                      '12wre_med', '12wre_max', '12wre_med_prophet', '12wre_max_prophet',
    #                                      '12wre_med_arima', '12wre_max_arima',
    #                                      'cum_error', 'cum_quantity', 'period_days'])
    #
    # if ('dir_name' in kwargs.keys()):
    #     dir_name = kwargs.get('dir_name')
    #     # model fit(comment)
    #     train = train[:-test_points]
    #     three_dim_save_plot(x1=train.ds, y1=train.y, y1_label='Train_observed',
    #                         x2=train.ds, y2=np.array(pred_train.predicted_mean), y2_label='ARIMA_fit',
    #                         x3=train.ds, y3=pf_train_pred.yhat, y3_label='Prophet_fit',
    #                         # x4=output_result.ds, y4=output_result.y, y4_label='Test_observed',
    #                         # x5=output_result.ds, y5=output_result.y_ARIMA, y5_label='ARIMA_pred',
    #                         # x6=output_result.ds, y6=output_result.y_Prophet, y6_label='Prophet_pred',
    #                         # x7=output_result.ds, y7=output_result.y_Ensembled, y7_label='Ensembled_pred',
    #                         xlable="Date", ylable="Quantity", title="model_fit",
    #                         dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
    #
    #     # model pred(comment)
    #     four_dim_save_plot(x1=output_result.ds, y1=output_result.y, y1_label='Test_observed',
    #                        x2=output_result.ds, y2=output_result.y_ARIMA, y2_label='ARIMA_pred',
    #                        x3=output_result.ds, y3=output_result.y_Prophet, y3_label='Prophet_pred',
    #                        x4=output_result.ds, y4=output_result.y_Ensembled, y4_label='Ensembled_pred',
    #                        xlable="Date", ylable="Quantity", title="model_pred",
    #                        dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
    #
    #     # Error plots(comment)
    #     weekly_ensm_model_error_plots(output_result=output_result, dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
    #
    # return output_error
