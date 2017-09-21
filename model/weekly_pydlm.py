from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *

def weekly_pydlm_model(prod, cus_no, mat_no, min_train_days=731, test_points=2, **kwargs):
    import pandas as pd
    import numpy as np
    import itertools
    import warnings
    import statsmodels.api as sm
    from fbprophet import Prophet
    from pydlm import dlm, trend, seasonality, dynamic, autoReg, longSeason
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

    # test and train data creation
    train = prod[
        prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):]
    # test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    # output_result = pd.DataFrame()

    train_pydlm = train.set_index('ds', drop=True)
    test_pydlm = test.set_index('ds', drop=True)

    # Modeling
    myDLM = dlm(train_pydlm.y)
    # add a first-order trend (linear trending) with prior covariance 1.0
    myDLM = myDLM + trend(1, name='lineTrend', w=1.0)
    # add a 7 day seasonality with prior covariance 1.0
    myDLM = myDLM + seasonality(52, name='52weeks', w=1.0)
    # add a 3 step auto regression
    myDLM = myDLM + autoReg(degree=2, data=train_pydlm.y, name='ar2', w=1.0)
    # show the added components
    myDLM.ls()

    # fit forward filter
    myDLM.fitForwardFilter()
    # fit backward smoother
    myDLM.fitBackwardSmoother()

    # plot the results
    myDLM.plot()
    # plot only the filtered results
    myDLM.turnOff('smoothed plot')
    myDLM.plot()
    # plot in one figure
    myDLM.turnOff('multiple plots')
    myDLM.plot()

    (predictMean, predictVar) = myDLM.predict(date=myDLM.n - 1,)
    (predictMean1, predictVar1) = myDLM.continuePredict()
    print(predictMean.item((0,0)))
    print(predictMean1.item((0,0)))
    # print(type(predictVar))

