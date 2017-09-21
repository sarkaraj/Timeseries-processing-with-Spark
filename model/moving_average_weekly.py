from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
# from transform_data.data_transform import *
import transform_data.pandas_support_func as pd_func

def moving_average_model_weekly(prod, cus_no, mat_no, weekly_window= 6, **kwargs):

    # If weekly data is false, monthly data is assumed

    import pandas as pd
    import numpy as np
    from dateutil import parser

    # data transform
    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    # save plot
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="raw_weekly_aggregated_quantity",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
    # Remove outlier
    if len(prod.y) >= 26:
        if ('dir_name' in kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size= 12, sigma= 2.5
                                      , dir_name=dir_name, mat_no=mat_no, cus_no=cus_no)
        else:
            prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size= 12, sigma= 2.5)

        # save plot
        if ('dir_name' in kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                              title="weekly_aggregated_quantity_outlier_replaced",
                              dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    prod['rolling_mean'] = pd.rolling_mean(prod['y'], window= weekly_window, min_periods= 1)
    pred = prod['rolling_mean'].iloc[-1]
    (output_result, rmse, mape) = weekly_moving_average_error_calc(data= prod, weekly_window= weekly_window)

    output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse, mape,
                                       np.nanmedian(output_result.rolling_6week_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_6week_percent_error))),
                                       np.nanmedian(output_result.rolling_12week_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_12week_percent_error))),
                                       output_result['Error_Cumsum'].iloc[-1],
                                       output_result['cumsum_quantity'].iloc[-1],
                                       ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
                                columns=['cus_no', 'mat_no', 'rmse', 'mape', 'wre_med_6', 'wre_max_6',
                                         'wre_med_12','wre_max_12', 'cum_error', 'cum_quantity', 'period_days'])

    output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
    _pred_result = {'yhat': pred}
    _pdt_cat = kwargs.get('pdt_cat')

    return cus_no, mat_no, output_error_dict, _pred_result, _pdt_cat