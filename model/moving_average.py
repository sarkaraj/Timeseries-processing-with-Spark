from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
from transform_data.data_transform import *

def add_months(sourcedate,months):
    import datetime
    import calendar
    month = sourcedate.month - 1 + months
    year = int(sourcedate.year + month / 12)
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    return datetime.date(year,month,day)

def moving_average_model(prod, cus_no, mat_no, weekly_data = True,
                         weekly_window= 6, monthly_window = 3, pred_points = 2, **kwargs):

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

    if weekly_data == True:
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

        pred_df = pd.DataFrame()
        pred_df['y'] = prod['y']
        for i in range(pred_points):
            pred_df['rolling_mean'] = pd.rolling_mean(pred_df['y'], window= weekly_window, min_periods= 1)
            pred_temp = pd.DataFrame([pred_df['rolling_mean'].iloc[-1]], columns=['y'])
            pred_df = pred_df.drop('rolling_mean', axis= 1)
            pred_df = pd.concat([pred_df,pred_temp],axis= 0, ignore_index= True)


        pred = np.array(pred_df['y'].iloc[-pred_points:])

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
        return (output_error, pred)
    elif weekly_data == False:
        prod = get_monthly_aggregate_per_product(prod)
        # save plot
        if ('dir_name' in kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            one_dim_save_plot(x=prod.ds, y=prod.y, xlable="Date", ylable="quantity",
                              title="raw_monthly_aggregated_quantity",
                              dir_name=dir_name, cus_no=cus_no, mat_no=mat_no)
        # Remove outlier
        if len(prod.y) >= 12:
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

        pred = np.array(pred_df['y'].iloc[-pred_points:])
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

        return (output_error, pred)

