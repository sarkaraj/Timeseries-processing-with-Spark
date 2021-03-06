from model.ma_outlier import *
from transform_data.data_transform import *
from model.error_calculator import *
from model.save_images import *


def monthly_prophet_model(prod, cus_no, mat_no, min_train_days=365, test_points=1, **kwargs):
    """
           Prod: weekly aggregated dataframe containing
               columns: customernumber, matnr, dt_week, quantity, p_ind_quantity
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

    prod = get_monthly_aggregate_per_product(prod)

    # save plot (comment)
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="raw_weekly_aggregated_quantity",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # Remove outlier
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive= True, window_size= 6, sigma= 2.5
                                  ,dir_name=dir_name, mat_no=mat_no, cus_no=cus_no)
    else:
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive= True, window_size= 6, sigma= 2.5)

    # save plot (comment)
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="weekly_aggregated_quantity_outlier_replaced",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # prod = get_monthly_aggregate_per_product

    # Remove outlier
    prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size=6, sigma=2.5)

    # test and train data creation
    train = prod[
        prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

    print(train)
    print(test)

    output_result = pd.DataFrame()

    # incremental test
    while (len(test) > 0):
        print("Enter While loop")
        # prophet model
        m = Prophet(weekly_seasonality=False, yearly_seasonality=False,
                    changepoint_prior_scale=1)
        pr_fit = m.fit(train);
        # TODO : Parameterize seasonality_prior_scale and changepoint_prior_scale - distribute this.
        # TODO : changepoint_prior_scale --> (1 to 10) + seasonality_prior_scale --> (0.1 to 1).

        # creating pred train and test data frame
        past = pr_fit.make_future_dataframe(periods=0, freq='M')
        future = pd.DataFrame(test['ds'])
        pf_train_pred = pr_fit.predict(past)
        pf_test_pred = pr_fit.predict(future)
        pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
        pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])

        # ceating test and train emsembled result
        #test result
        result_test = test
        result_test['y_Prophet'] = np.array(pf_test_pred.yhat)
        result_test.loc[(result_test['y_Prophet'] < 0), 'y_Prophet'] = 0

        train = prod[:(max(train.index)+1+test_points)]
        test = prod[(max(train.index)+1):(max(train.index)+1+test_points)]
        # rem_data = prod[(max(train.index)+test_points):]

        output_result = pd.concat([output_result,result_test], axis=0)

    output_result = monthly_prophet_model_error_calculator(output_result)

    print(output_result)
    #############################
    # m_ = Prophet(weekly_seasonality=False, yearly_seasonality=True,
    #              changepoint_prior_scale=2)
    # print(prod.tail())
    # m_.fit(prod);
    # pred_ds = m_.make_future_dataframe(periods=2, freq='M').tail(2)
    # pred_ds.ds = pred_ds.ds.map(lambda x: x + dt.timedelta(days=15))
    #
    # _prediction_temp = m_.predict(pred_ds)[['ds', 'yhat']]
    # print(_prediction_temp)
    ##########################

    output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                                       mape_calculator(output_result.y_Prophet, output_result.y),
                                       np.nanmedian(output_result.rolling_3month_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_3month_percent_error))),
                                       np.nanmedian(output_result.rolling_4month_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_4month_percent_error))),
                                       output_result['Error_Cumsum'].iloc[-1],
                                       output_result['cumsum_quantity'].iloc[-1],
                                       ((np.amax(output_result.ds) - np.amin(output_result.ds)).days+30)]],
                                columns=['cus_no', 'mat_no', 'rmse', 'mape','3mre_med', '3mre_max','4mre_med', '4mre_max',
                                         'cum_error', 'cum_quantity', 'period_days'])

    print(output_error)
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        # model fit
        train = train[:-test_points]
        four_dim_save_plot(x1=train.ds, y1=train.y, y1_label='Train_observed',
                          x2=train.ds, y2=pf_train_pred.yhat, y2_label='Prophet_fit',
                          x3=output_result.ds, y3=output_result.y, y3_label='Test_observed',
                          x4=output_result.ds, y4=output_result.y_Prophet, y4_label='Prophet_pred',
                          xlable="Date", ylable="Quantity", title="model_fit",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

        # plot cumulative error
        try:
            one_dim_save_plot(x= output_result.ds, y= output_result.Error_Cumsum,
                              xlable="Date", ylable="% Cumulative Error", title="cumulative_error",
                              dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

            # plot cumulative error
            one_dim_save_plot(x=output_result.ds, y=output_result.rolling_3month_percent_error,
                              xlable="Date", ylable="% 3 Month Rolling Error", title="3month_rolling_error",
                              dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
        except ValueError:
            print("No points to plot")
    return (output_error)
