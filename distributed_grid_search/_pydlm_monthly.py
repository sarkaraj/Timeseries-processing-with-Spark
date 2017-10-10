from model.ma_outlier import *
from model.error_calculator import *
import distributed_grid_search.properties as p_model
from transform_data.data_transform import *

def run_pydlm_monthly(cus_no, mat_no, prod, param, **kwargs):
    import pandas as pd
    import numpy as np
    from dateutil import parser
    from fbprophet import Prophet
    from pydlm import dlm, trend, seasonality, dynamic, autoReg, longSeason, modelTuner

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

    # model parameters
    trend_degree = param.get('trend_degree')
    trend_w = param.get('trend_w')
    seasonality_w = param.get('seasonality_w')
    ar_degree = param.get('ar_degree')
    ar_w = param.get('ar_w')

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
        train_pydlm = train.set_index('ds', drop=True)
        test_pydlm = test.set_index('ds', drop=True)

        # Modeling
        myDLM = dlm(train_pydlm.y)
        # add a first-order trend (linear trending) with prior covariance 1.0
        myDLM = myDLM + trend(degree=trend_degree, name='trend', w=trend_w)
        # # add a 12 month seasonality with prior covariance 1.0
        myDLM = myDLM + seasonality(12, name='12month', w= seasonality_w)
        # # add a 3 step auto regression
        myDLM = myDLM + autoReg(degree=ar_degree, data=train_pydlm.y, name='ar', w=ar_w)

        myDLM.fit()

        (predictMean, predictVar) = myDLM.predict(date=myDLM.n - 1)
        pred_test = np.array(predictMean.item((0, 0)))
        for i in range(test_points-1):
            (predictMean, predictVar) = myDLM.continuePredict()
            pred_test = np.append(pred_test,predictMean.item((0, 0)))

        result_test = test
        result_test['y_pydlm'] = pred_test
        result_test.loc[(result_test['y_pydlm'] < 0), 'y_pydlm'] = 0

        train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        # rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

        output_result = pd.concat([output_result, result_test], axis=0)


