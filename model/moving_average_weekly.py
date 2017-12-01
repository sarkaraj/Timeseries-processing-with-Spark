from model.ma_outlier import *
from model.error_calculator import *
from model.save_images import *
import distributed_grid_search.properties as p_model
# from transform_data.data_transform import *
import transform_data.pandas_support_func as pd_func
from transform_data.data_transform import gregorian_to_iso

def add_days(current_date):
    import datetime
    delta = datetime.timedelta(days=7)
    new_date = current_date + delta

    return new_date

def _get_pred_dict_MA_w(prediction):
    prediction_df_temp = prediction.set_index('ds', drop=True)
    prediction_df_temp.index = prediction_df_temp.index.map(lambda x: x.strftime('%Y-%m-%d'))
    pred = prediction_df_temp.to_dict(orient='index')
    _final = {(gregorian_to_iso(key.split("-"))[1], gregorian_to_iso(key.split("-"))[0]): float(pred.get(key).get('yhat'))
              for key in pred.keys()}
    return _final

def moving_average_model_weekly(prod, cus_no, mat_no, baseline = False, **kwargs):

    # always define min_train_days when used for baseline

    import pandas as pd
    import numpy as np
    from dateutil import parser

    if ('weekly_window' in kwargs.keys()):
        weekly_window = kwargs.get('weekly_window')
    else:
        weekly_window = 6

    if ('pred_points' in kwargs.keys()):
        pred_points = kwargs.get('pred_points')
    else:
        pred_points = p_model.pred_points

    if (kwargs.has_key('min_train_days')):
        min_train_days = kwargs.get('min_train_days')
    else:
        min_train_days = p_model.min_train_days

    # data transform
    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    # prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

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
        pred_df['rolling_mean'] = pd.rolling_mean(pred_df['y'], window=weekly_window, min_periods=1)
        pred_temp = pd.DataFrame([pred_df['rolling_mean'].iloc[-1]], columns=['y'])
        pred_df = pred_df.drop('rolling_mean', axis=1)
        pred_df = pd.concat([pred_df, pred_temp], axis=0, ignore_index=True)

    #forecast
    pred = np.array(pred_df['y'].iloc[-pred_points:]).tolist()
    ds = np.array([prod['ds'].iloc[-1]])

    for i in range(pred_points):
        ds = np.append(ds, add_days(ds[-1]))[-pred_points:]

    final_pred = _get_pred_dict_MA_w(pd.DataFrame({'ds': ds, 'yhat': pred}))

    (output_result, rmse, mape) = weekly_moving_average_error_calc(data=prod, weekly_window=weekly_window,
                                                                   baseline = baseline, min_train_days = min_train_days)

    output_error = pd.DataFrame(data=[[cus_no, mat_no, rmse, mape,
                                       np.nanmedian(np.absolute(np.array(output_result.rolling_6week_percent_error))),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_6week_percent_error))),
                                       np.nanmean(np.absolute(np.array(output_result.rolling_6week_percent_error))),
                                       np.nanmean(np.absolute(np.array(output_result.rolling_6week_quantity))),
                                       np.nanmedian(np.absolute(np.array(output_result.rolling_12week_percent_error))),
                                       np.nanmax(np.absolute(np.array(output_result.rolling_12week_percent_error))),
                                       np.nanmean(np.absolute(np.array(output_result.rolling_12week_percent_error))),
                                       np.nanmean(np.absolute(np.array(output_result.rolling_12week_quantity))),
                                       output_result['Error_Cumsum'].iloc[-1],
                                       output_result['cumsum_quantity'].iloc[-1],
                                       ((np.amax(output_result.ds) - np.amin(output_result.ds)).days + 7)]],
                                columns=['cus_no', 'mat_no', 'rmse', 'mape',
                                         'wre_med_6', 'wre_max_6', 'wre_mean_6', 'quantity_mean_6',
                                         'wre_med_12', 'wre_max_12', 'wre_mean_12', 'quantity_mean_12',
                                         'cum_error', 'cum_quantity',
                                         'period_days'])

    output_error_dict = pd_func.extract_elems_from_dict(output_error.to_dict(orient='index'))
    # _pred_result = {'yhat': list(pred)}
    _pdt_cat = kwargs.get('pdt_cat')

    # return cus_no, mat_no, output_error_dict, pred, _pdt_cat
    return cus_no, mat_no, output_error_dict, final_pred, _pdt_cat

if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    import datetime

    data = {'ds': ['2014-05-01', '2014-05-01', '2014-05-02',
                   '2014-05-02', '2014-05-02', '2014-05-02',
                   '2014-05-03', '2014-05-03', '2014-05-04',
                   '2017-10-11'],
            'battle_deaths': [34, 25, 26, 15, 15, 14, 26, 25, 62, 41]}
    df = pd.DataFrame(data, columns=['ds', 'battle_deaths'])
    df.ds = df.ds.map(lambda x: pd.to_datetime(x).date())
    # print df

    a = df['ds'].iloc[-1]
    # print "a"
    # print a
    # print type(a)

    ds = np.array([a])
    # print "ds"
    # print ds
    # print type(ds)
    #
    # print ds[-1]

    print ("*************************")
    for i in range(3):
        ds = np.append(ds, add_days(ds[-1]))[-3:]

    print (ds)
    print (len(ds))

    temp_d = pd.DataFrame({'ds': ds, 'yhat': [1, 2, 3]})
    print (temp_d)
    print (_get_pred_dict_MA_w(temp_d))
    print (gregorian_to_iso([2017,10,18]))
