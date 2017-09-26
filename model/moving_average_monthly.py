from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
import distributed_grid_search.properties as p_model
from transform_data.data_transform import *
import transform_data.pandas_support_func as pd_func


def moving_average_model_monthly(prod, cus_no, mat_no, **kwargs):
    # If weekly data is false, monthly data is assumed

    import pandas as pd
    import numpy as np
    from dateutil import parser

    _pdt_cat = kwargs.get('pdt_cat')

    if ('monthly_window' in kwargs.keys()):
        monthly_window = kwargs.get('monthly_window')
    else:
        monthly_window = 3

    if ('pred_points' in kwargs.keys()):
        pred_points = kwargs.get('pred_points')
    else:
        pred_points = p_model.pred_points_monthly

    # data transform
    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    prod = get_monthly_aggregate_per_product(prod)
    # save plot
    if ('dir_name' in kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                          title="raw_monthly_aggregated_quantity",
                          dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
    # Remove outlier
    if len(prod.y) >= 12 and _pdt_cat.get('category') not in ('IX', 'X'):
        if ('dir_name' in kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size=6, sigma= 2.5
                                      ,dir_name=dir_name, mat_no=mat_no, cus_no=cus_no)
        else:
            prod = ma_replace_outlier(data=prod, n_pass=3, aggressive=True, window_size= 6, sigma= 2.5)

        # save plot
        if ('dir_name' in kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                              title="monthly_aggregated_quantity_outlier_replaced",
                              dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)

    pred_df = pd.DataFrame()
    pred_df['y'] = prod['y']
    for i in range(pred_points):
        pred_df['rolling_mean'] = pd.rolling_mean(pred_df['y'], window=monthly_window, min_periods=1)
        pred_temp = pd.DataFrame([pred_df['rolling_mean'].iloc[-1]], columns=['y'])
        pred_df = pred_df.drop('rolling_mean', axis=1)
        pred_df = pd.concat([pred_df, pred_temp], axis=0, ignore_index=True)

    pred = np.array(pred_df['y'].iloc[-pred_points:]).tolist()
    (output_result, rmse, mape) = monthly_moving_average_error_calc(data=prod, monthly_window=monthly_window)

    output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse, mape,
                                       np.nanmedian(output_result.rolling_3month_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_3month_percent_error))),
                                       np.nanmedian(output_result.rolling_4month_percent_error),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_4month_percent_error))),
                                       output_result['Error_Cumsum'].iloc[-1],
                                       output_result['cumsum_quantity'].iloc[-1],
                                       ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 30)]],
                                columns=['cus_no', 'mat_no', 'rmse', 'mape', 'mre_med_3', 'mre_max_3',
                                         'mre_med_4', 'mre_max_4', 'cum_error', 'cum_quantity', 'period_days'])

    output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
    # _pred_result = {'yhat': list(pred)}


    return cus_no, mat_no, output_error_dict, pred, _pdt_cat
