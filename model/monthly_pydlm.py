from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
from transform_data.data_transform import *

def monthly_pydlm_model(prod, cus_no, mat_no, min_train_days=731, test_points=1, **kwargs):
    """

    :param prod: data
    :param cus_no: customer number
    :param mat_no: product number
    :param min_train_days: Min training data from where cross validation starts
    :param test_points: number of points ahead prediction(for the time max is 1): need to include
    :param kwargs: provide dir_name to save images and error excel
    :return: returns a data frame containing cross validation result
    """

    import pandas as pd
    import numpy as np
    import itertools
    import warnings
    import statsmodels.api as sm
    from fbprophet import Prophet
    from pydlm import dlm, trend, seasonality, dynamic, autoReg, longSeason, modelTuner
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
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size=6, sigma=2.5
                                  ,dir_name=dir_name, mat_no=mat_no, cus_no=cus_no)
    else:
        prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size=6, sigma= 2.5)

    # save plot (comment)
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="weekly_aggregated_quantity_outlier_replaced",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    # test and train data creation
        # test and train data creation
    train = prod[
        prod.ds <= (
        np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    output_result = pd.DataFrame()
    output_error = pd.DataFrame(columns=['cus_no', 'mat_no', 'rmse', 'mape', '3mre_med', '3mre_max', '4mre_med',
                                         '4mre_max', 'cum_error', 'cum_quantity', 'period_days'])
    try:
        while (len(rem_data.ds) >= test_points):

            train_pydlm = train.set_index('ds', drop=True)
            # test_pydlm = test.set_index('ds', drop=True)

            # Modeling
            myDLM = dlm(train_pydlm.y)
            # add a first-order trend (linear trending) with prior covariance 1.0
            myDLM = myDLM + trend(degree=3, name='quadratic', w=1.0)
            # # add a 12 month seasonality with prior covariance 1.0
            myDLM = myDLM + seasonality(12, name='12month', w=0.0)
            # # add a 3 step auto regression
            myDLM = myDLM + autoReg(degree=3, data=train_pydlm.y, name='ar2', w=1.0)
            # # show the added components
            # myDLM.ls()

            # # fit forward filter
            # myDLM.fitForwardFilter()
            # # fit backward smoother
            # myDLM.fitBackwardSmoother()


            # myTuner = modelTuner(method='gradient_descent', loss='mse')
            # tunedDLM = myTuner.tune(myDLM, maxit=100)
            # tuned_discounts = myTuner.getDiscounts()
            # print(tuned_discounts)
            # tunedDLM.fit()
            myDLM.fit()
            # myDLM.tune()

            # myDLM.plot()

            # plot the results
            # if ('dir_name' in kwargs.keys()):
            #     dir_name = kwargs.get('dir_name')
            #     fig = plt.figure()
            #     myDLM.plot()
            #     # fig = plot.figure()
            #     plt.savefig(dir_name +str(cus_no)+"_"+str(mat_no)+ '_model_fit.png')
            #     plt.close(fig)
            # # plot only the filtered results
            # myDLM.turnOff('smoothed plot')
            # myDLM.plot()
            # # plot in one figure
            # myDLM.turnOff('multiple plots')
            # myDLM.plot()

            (predictMean, predictVar) = myDLM.predict(date=myDLM.n - 1)
            # (predictMean1, predictVar1) = myDLM.continuePredict()
            # print(predictMean.item((0,0)))
            # print(predictMean1.item((0,0)))
            # print(type(predictVar))

            result_test = test
            result_test['y_pydlm'] = np.array([predictMean.item((0,0))])
            result_test.loc[(result_test['y_pydlm'] < 0), 'y_pydlm'] = 0

            print('Next Test Starts...')
            train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
            test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
            rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

            output_result = pd.concat([output_result, result_test], axis=0)

        output_result = monthly_pydlm_model_error_calculator(output_result)

        output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse_calculator(output_result.y_pydlm, output_result.y),
                                           mape_calculator(output_result.y_pydlm, output_result.y),
                                           np.nanmedian(output_result.rolling_3month_percent_error),
                                           np.nanmax(np.absolute(np.array(output_result.rolling_3month_percent_error))),
                                           np.nanmedian(output_result.rolling_4month_percent_error),
                                           np.nanmax(np.absolute(np.array(output_result.rolling_4month_percent_error))),
                                           output_result['Error_Cumsum'].iloc[-1],
                                           output_result['cumsum_quantity'].iloc[-1],
                                           ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 30)]],
                                    columns=['cus_no', 'mat_no', 'rmse', 'mape', '3mre_med', '3mre_max', '4mre_med',
                                             '4mre_max', 'cum_error', 'cum_quantity', 'period_days'])

        if ('dir_name' in kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            try:
                # plot cumulative error
                two_dim_save_plot(x1=output_result.ds, y1=output_result.y_pydlm, y1_label= 'pydlm_pred',
                                  x2=output_result.ds, y2=output_result.y, y2_label = 'observed',
                                  xlable="Date", ylable="quantity", title="pydlm_prediction",
                                  dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
                # plot cumulative error
                one_dim_save_plot(x=output_result.ds, y=output_result.Error_Cumsum,
                                  xlable="Date", ylable="% Cumulative Error", title="cumulative_error",
                                  dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
                # plot cumulative error
                one_dim_save_plot(x=output_result.ds, y=output_result.rolling_3month_percent_error,
                                  xlable="Date", ylable="% 3 Month Rolling Error", title="3month_rolling_error",
                                  dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
            except ValueError:
                print("No points to plot")
    except np.linalg.linalg.LinAlgError:
        print("could not fit")
    return (output_error)