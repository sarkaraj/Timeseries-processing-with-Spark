from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *

def moving_average_model(prod, cus_no, mat_no,weekly_data = True,
                         weekly_window= 6, monthly_window = 3, **kwargs):

    # If weekly data is false, monthly data is assumed

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

    if weekly_data == True:
        prod['rolling_mean'] = pd.rolling_mean(prod.y, window= weekly_window, min_periods= 1)
        pred = prod['rolling_mean'].iloc[-1]
    if weekly_data == False:
        prod['rolling_mean'] = pd.rolling_mean(prod.y, window= monthly_window, min_periods= 1)
        pred = prod['rolling_mean'].iloc[-1]

    return pred

