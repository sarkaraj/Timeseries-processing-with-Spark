def weekly_ensm_model_error_calc(data):

    import pandas as pd
    import numpy as np

    # Calculate total ACTUAL Quantity
    data['cumsum_quantity'] = data.y.cumsum()

    # NOT REQUIRED for distributed
    data['Error'] = np.subtract(data.y_Ensembled, data.y)
    data['Error_Cumsum'] = data.Error.cumsum() / data.y.cumsum() * 100

    data['Error_prophet'] = np.subtract(data.y_Prophet, data.y)
    data['Error_Cumsum_prophet'] = data.Error_prophet.cumsum() / data.y.cumsum() * 100

    data['Error_arima'] = np.subtract(data.y_ARIMA, data.y)
    data['Error_Cumsum_arima'] = data.Error_arima.cumsum() / data.y.cumsum() * 100


    # Calculate Rolling 6-week and 12-week error for Ensemble
    data['rolling_6week_error'] = pd.rolling_sum(data['Error'], window=6, min_periods=6)
    data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    data['rolling_6week_percent_error'] = data['rolling_6week_error'] / data[
        'rolling_6week_y'] * 100

    data['rolling_12week_error'] = pd.rolling_sum(data['Error'], window=12, min_periods=12)
    data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    data['rolling_12week_percent_error'] = data['rolling_12week_error'] / data[
        'rolling_12week_y'] * 100

    # Calculate Rolling 6-week and 12-week error for Arima
    data['rolling_6week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=6, min_periods=6)
    data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    data['rolling_6week_percent_error_arima'] = data['rolling_6week_error_arima'] / data[
        'rolling_6week_y'] * 100

    data['rolling_12week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=12,
                                                                 min_periods=12)
    data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    data['rolling_12week_percent_error_arima'] = data['rolling_12week_error_arima'] / data[
        'rolling_12week_y'] * 100

    # Calculate Rolling 6-week and 12-week error for Prophet
    data['rolling_6week_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=6,
                                                                  min_periods=6)
    data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    data['rolling_6week_percent_error_prophet'] = data['rolling_6week_error_prophet'] / data[
        'rolling_6week_y'] * 100

    data['rolling_12week_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=12,
                                                                   min_periods=12)
    data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    data['rolling_12week_percent_error_prophet'] = data['rolling_12week_error_prophet'] / \
                                                            data['rolling_12week_y'] * 100

    return (data)

def monthly_prophet_model_error_calculator(data):

    import pandas as pd
    import numpy as np

    data['cumsum_quantity'] = data.y.cumsum()

    data['Error'] = np.subtract(data.y_Prophet, data.y)
    data['Error_Cumsum'] = data.Error.cumsum() / data.y.cumsum() * 100

    data['rolling_3month_error'] = pd.rolling_sum(data['Error'], window=3, min_periods=3)
    data['rolling_3month_y'] = pd.rolling_sum(data['y'], window=3, min_periods=3)
    data['rolling_3month_percent_error'] = data['rolling_3month_error'] / data[
        'rolling_3month_y'] * 100

    data['rolling_4month_error'] = pd.rolling_sum(data['Error'], window=4, min_periods=4)
    data['rolling_4month_y'] = pd.rolling_sum(data['y'], window=4, min_periods=4)
    data['rolling_4month_percent_error'] = data['rolling_4month_error'] / data[
        'rolling_4month_y'] * 100

    return (data)

def monthly_pydlm_model_error_calculator(data):

    import pandas as pd
    import numpy as np

    data['cumsum_quantity'] = data.y.cumsum()

    data['Error'] = np.subtract(data.y_pydlm, data.y)
    data['Error_Cumsum'] = data.Error.cumsum() / data.y.cumsum() * 100

    data['rolling_3month_error'] = pd.rolling_sum(data['Error'], window=3, min_periods=3)
    data['rolling_3month_y'] = pd.rolling_sum(data['y'], window=3, min_periods=3)
    data['rolling_3month_percent_error'] = data['rolling_3month_error'] / data[
        'rolling_3month_y'] * 100

    data['rolling_4month_error'] = pd.rolling_sum(data['Error'], window=4, min_periods=4)
    data['rolling_4month_y'] = pd.rolling_sum(data['y'], window=4, min_periods=4)
    data['rolling_4month_percent_error'] = data['rolling_4month_error'] / data[
        'rolling_4month_y'] * 100

    return (data)

def rmse_calculator(y_forecasted,y_truth):

    rmse = (((y_forecasted - y_truth) ** 2).mean())**0.5

    return (round(rmse, 2))

def mae_calculator(y_forecasted,y_truth):

    import numpy as np
    mae = (np.abs(y_forecasted - y_truth)).mean()

    return (round(mae, 2))

def mape_calculator(y_forecasted,y_truth):

    import numpy as np

    mape = np.mean(np.abs((y_truth - y_forecasted) / y_truth)) * 100

    return (round(mape, 2))

def weekly_moving_average_error_calc(data, weekly_window, baseline = False, min_train_days = 365):
    '''
    :param data: weekly aggregated data
    :param weekly_window: rolling window
    :param baseline: true if the model is used for baseline evaluation
    :param min_train_days: only used if baseline is true
    :return: data frame of errors
    '''

    import numpy as np
    import pandas as pd

    data['rolling_mean'] = pd.rolling_mean(data['y'].shift(), window=weekly_window, min_periods=1)

    if baseline == True:
        train = data[data.ds <= (np.amax(data.ds) - pd.DateOffset(days=(np.amax(data.ds) - np.amin(data.ds)).days - min_train_days))]
        test = data[(np.amax(np.array(train.index)) + 1):]
        data_pred = data.tail(len(test)).reset_index(drop=True)
    else:
        if len(data.y) > 52:
            data_pred = data.tail(52).reset_index(drop=True)
        else:
            data_pred = data.reset_index(drop=True)

    data_pred['cumsum_quantity'] = data_pred.y.cumsum()

    data_pred['Error'] = np.subtract(data_pred.rolling_mean, data_pred.y)
    data_pred['Error_Cumsum'] = data_pred.Error.cumsum() / data_pred.y.cumsum() * 100

    data_pred['rolling_6week_error'] = pd.rolling_sum(data_pred['Error'], window=6, min_periods=6)
    data_pred['rolling_6week_y'] = pd.rolling_sum(data_pred['y'], window=6, min_periods=6)
    data_pred['rolling_6week_percent_error'] = data_pred['rolling_6week_error'] / data_pred[
        'rolling_6week_y'] * 100
    data_pred['rolling_6week_quantity'] = pd.rolling_sum(data_pred['y'], window=6, min_periods=6)

    data_pred['rolling_12week_error'] = pd.rolling_sum(data_pred['Error'], window=12, min_periods=12)
    data_pred['rolling_12week_y'] = pd.rolling_sum(data_pred['y'], window=12, min_periods=12)
    data_pred['rolling_12week_percent_error'] = data_pred['rolling_12week_error'] / data_pred[
        'rolling_12week_y'] * 100
    data_pred['rolling_12week_quantity'] = pd.rolling_sum(data_pred['y'], window=12, min_periods=12)

    rmse = rmse_calculator(y_forecasted= data_pred.rolling_mean,y_truth= data_pred.y)

    mape = mape_calculator(y_forecasted= data_pred.rolling_mean, y_truth=data_pred.y)

    return(data_pred, rmse, mape)

def monthly_moving_average_error_calc(data, monthly_window, baseline = False, min_train_days = 365):

    import numpy as np
    import pandas as pd

    data['rolling_mean'] = pd.rolling_mean(data['y'].shift(), window=monthly_window, min_periods=1)

    if baseline == True:
        train = data[data.ds <= (np.amax(data.ds) - pd.DateOffset(days=(np.amax(data.ds) - np.amin(data.ds)).days - min_train_days))]
        test = data[(np.amax(np.array(train.index)) + 1):]
        data_pred = data.tail(len(test)).reset_index(drop=True)
    else:
        if len(data.y) > 12:
            data_pred = data.tail(12).reset_index(drop=True)
        else:
            data_pred = data.reset_index(drop=True)

    data_pred['cumsum_quantity'] = data_pred.y.cumsum()

    data_pred['Error'] = np.subtract(data_pred.rolling_mean, data_pred.y)
    data_pred['Error_Cumsum'] = data_pred.Error.cumsum() / data_pred.y.cumsum() * 100

    data_pred['rolling_3month_error'] = pd.rolling_sum(data_pred['Error'], window=3, min_periods=3)
    data_pred['rolling_3month_y'] = pd.rolling_sum(data_pred['y'], window=3, min_periods=3)
    data_pred['rolling_3month_percent_error'] = data_pred['rolling_3month_error'] / data_pred[
        'rolling_3month_y'] * 100
    data_pred['rolling_3month_quantity'] = pd.rolling_sum(data_pred['y'], window=3, min_periods=3)

    data_pred['rolling_4month_error'] = pd.rolling_sum(data_pred['Error'], window=4, min_periods=4)
    data_pred['rolling_4month_y'] = pd.rolling_sum(data_pred['y'], window=4, min_periods=4)
    data_pred['rolling_4month_percent_error'] = data_pred['rolling_4month_error'] / data_pred[
        'rolling_4month_y'] * 100
    data_pred['rolling_4month_quantity'] = pd.rolling_sum(data_pred['y'], window=4, min_periods=4)

    data_pred['rolling_6month_error'] = pd.rolling_sum(data_pred['Error'], window=6, min_periods=6)
    data_pred['rolling_6month_y'] = pd.rolling_sum(data_pred['y'], window=6, min_periods=6)
    data_pred['rolling_6month_percent_error'] = data_pred['rolling_6month_error'] / data_pred[
        'rolling_6month_y'] * 100
    data_pred['rolling_6month_quantity'] = pd.rolling_sum(data_pred['y'], window=6, min_periods=6)

    data_pred['rolling_12month_error'] = pd.rolling_sum(data_pred['Error'], window=12, min_periods=12)
    data_pred['rolling_12month_y'] = pd.rolling_sum(data_pred['y'], window=12, min_periods=12)
    data_pred['rolling_12month_percent_error'] = data_pred['rolling_12month_error'] / data_pred[
        'rolling_12month_y'] * 100
    data_pred['rolling_12month_quantity'] = pd.rolling_sum(data_pred['y'], window=12, min_periods=12)

    rmse = rmse_calculator(y_forecasted= data_pred.rolling_mean,y_truth= data_pred.y)

    mape = mape_calculator(y_forecasted= data_pred.rolling_mean, y_truth=data_pred.y)

    return(data_pred, rmse, mape)


if __name__ == "__main__":

    import pandas as pd
    import numpy as np

    df = pd.DataFrame({'actual': [1, 2, 5], 'predicted': [2, 3, 0]})

    print(df)

    print(mae_calculator(df['actual'], df['predicted']))

    print(np.mean([1, 1, 5]))