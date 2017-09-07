from ma_outlier import *
from data_transform import *
from error_calculator import *
from save_images import *

def monthly_prophet_model(prod, cus_no, mat_no, dir_name, min_train_days=731, test_points=1):
    """
           Prod: weekly aggregated dataframe containing
               columns: customernumber, matnr, dt_week, quantity, p_ind_quantity
           dir_name: where to save files

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
    one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                      title="raw_weekly_aggregated_quantity",
                      dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # Remove outlier
    prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True,
                              dir_name=dir_name, mat_no=mat_no, cus_no=cus_no)  # comment if no image required

    # save plot (comment)
    one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                      title="weekly_aggregated_quantity_outlier_replaced",
                      dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    prod = get_monthly_aggregate_per_product(prod)

    # test and train data creation
    train = prod[
        prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

    output_result = pd.DataFrame()

    # incremental test
    while (len(rem_data.ds) >= test_points):
        # prophet model
        m = Prophet(weekly_seasonality=False, yearly_seasonality=True, changepoint_prior_scale=2,
                    seasonality_prior_scale=0.1)
        m.fit(train);

        # creating pred train and test data frame
        past = m.make_future_dataframe(periods=0, freq='M')
        future = pd.DataFrame(test['ds'])
        pf_train_pred = m.predict(past)
        pf_test_pred = m.predict(future)
        pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
        pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])

        # ceating test and train emsembled result
        #test result
        result_test = test
        result_test['y_Prophet'] = np.array(pf_test_pred.yhat)

        train = prod[:(max(train.index)+1+test_points)]
        test = prod[(max(train.index)+1):(max(train.index)+1+test_points)]
        rem_data = prod[(max(train.index)+test_points):]

        output_result = pd.concat([output_result,result_test], axis=0)

    output_result = monthly_prophet_model_error_calculator(output_result)

    output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse_calculator(output_result.y_Prophet, output_result.y),
                                       mape_calculator(output_result.y_Prophet, output_result.y),
                                       np.nanmedian(output_result.rolling_3month_percent_error),
                                       np.nanmax(np.absolute(output_result.rolling_3month_percent_error)),
                                       output_result['Error_Cumsum'].iloc[-1],
                                       output_result['cumsum_quantity'].iloc[-1],
                                       (np.amax(output_result.ds) - np.amin(output_result.ds)).days]],
                                columns=['cus_no', 'mat_no', 'rmse', 'mape','6wre_med', '6wre_max',
                                         'cum_error', 'cum_quantity', 'period_days'])

    # model fit(comment)
    train = train[:-test_points]
    four_dim_save_plot(x1=train.ds, y1=train.y, y1_label='Train_observed',
                      x2=train.ds, y2=pf_train_pred.yhat, y2_label='Prophet_fit',
                      x3=output_result.ds, y3=output_result.y, y3_label='Test_observed',
                      x4=output_result.ds, y4=output_result.y_Prophet, y4_label='Prophet_pred',
                      xlable="Date", ylable="Quantity", title="model_fit",
                      dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # plot cumulative error
    one_dim_save_plot(x= output_result.ds, y= output_result.Error_Cumsum,
                      xlable="Date", ylable="% Cumulative Error", title="cumulative_error",
                      dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # plot cumulative error
    one_dim_save_plot(x=output_result.ds, y=output_result.rolling_3month_percent_error,
                      xlable="Date", ylable="% 3 Month Rolling Error", title="3month_rolling_error",
                      dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    return (output_error)