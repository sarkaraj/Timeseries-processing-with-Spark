def weekly_arima_error_calc(_data):

    import pandas as pd
    import numpy as np

    data = _data.copy()

    # Calculate total ACTUAL Quantity
    data['cumsum_quantity'] = data.y.cumsum()

    # # NOT REQUIRED for distributed
    # data['Error'] = np.subtract(data.y_Ensembled, data.y)
    # data['Error_Cumsum'] = data.Error.cumsum() / data.y.cumsum() * 100
    #
    # data['Error_prophet'] = np.subtract(data.y_Prophet, data.y)
    # data['Error_Cumsum_prophet'] = data.Error_prophet.cumsum() / data.y.cumsum() * 100

    # Arima Error
    data['Error_arima'] = np.subtract(data.y_ARIMA, data.y)
    data['Error_Cumsum_arima'] = data.Error_arima.cumsum() / data.y.cumsum() * 100

    # Calculate Rolling 6-week and 12-week error for Arima
    data['rolling_6week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=6, min_periods=6)
    data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    data['rolling_6week_percent_error_arima'] = data['rolling_6week_error_arima'] / data[
        'rolling_6week_y'] * 100
    data['rolling_6week_quantity'] = pd.rolling_sum(data['y'], window=6, min_periods=6)

    data['rolling_12week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=12, min_periods=12)
    data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    data['rolling_12week_percent_error_arima'] = data['rolling_12week_error_arima'] / data[
        'rolling_12week_y'] * 100
    data['rolling_12week_quantity'] = pd.rolling_sum(data['y'], window=12, min_periods=12)

    data['rolling_24week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=24, min_periods=24)
    data['rolling_24week_y'] = pd.rolling_sum(data['y'], window=24, min_periods=24)
    data['rolling_24week_percent_error_arima'] = data['rolling_24week_error_arima'] / data[
        'rolling_24week_y'] * 100
    data['rolling_24week_quantity'] = pd.rolling_sum(data['y'], window=24, min_periods=24)

    data['rolling_48week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=48, min_periods=48)
    data['rolling_48week_y'] = pd.rolling_sum(data['y'], window=48, min_periods=48)
    data['rolling_48week_percent_error_arima'] = data['rolling_48week_error_arima'] / data[
        'rolling_48week_y'] * 100
    data['rolling_48week_quantity'] = pd.rolling_sum(data['y'], window=48, min_periods=48)

    # # Calculate Rolling 6-week and 12-week error for Ensemble
    # data['rolling_6week_error'] = pd.rolling_sum(data['Error'], window=6, min_periods=6)
    # data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    # data['rolling_6week_percent_error'] = data['rolling_6week_error'] / data[
    #     'rolling_6week_y'] * 100
    #
    # data['rolling_12week_error'] = pd.rolling_sum(data['Error'], window=12, min_periods=12)
    # data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    # data['rolling_12week_percent_error'] = data['rolling_12week_error'] / data[
    #     'rolling_12week_y'] * 100



    # # Calculate Rolling 6-week and 12-week error for Prophet
    # data['rolling_6week_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=6,
    #                                                               min_periods=6)
    # data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    # data['rolling_6week_percent_error_prophet'] = data['rolling_6week_error_prophet'] / data[
    #     'rolling_6week_y'] * 100
    #
    # data['rolling_12week_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=12,
    #                                                                min_periods=12)
    # data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    # data['rolling_12week_percent_error_prophet'] = data['rolling_12week_error_prophet'] / \
    #                                                         data['rolling_12week_y'] * 100

    data = data.replace([-np.inf, np.inf], np.nan)
    return data


def weekly_prophet_error_calc(_data):
    import pandas as pd
    import numpy as np

    data = _data.copy()

    # Calculate total ACTUAL Quantity
    data['cumsum_quantity'] = data.y.cumsum()

    data['Error_prophet'] = np.subtract(data.y_Prophet, data.y)
    data['Error_Cumsum_prophet'] = data.Error_prophet.cumsum() / data.y.cumsum() * 100

    # Calculate Rolling 6-week and 12-week error for Prophet
    data['rolling_6week_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=6,
                                                                  min_periods=6)
    data['rolling_6week_y'] = pd.rolling_sum(data['y'], window=6, min_periods=6)
    data['rolling_6week_percent_error_prophet'] = data['rolling_6week_error_prophet'] / data[
        'rolling_6week_y'] * 100
    data['rolling_6week_quantity'] = pd.rolling_sum(data['y'], window=6, min_periods=6)

    data['rolling_12week_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=12,
                                                                   min_periods=12)
    data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    data['rolling_12week_percent_error_prophet'] = data['rolling_12week_error_prophet'] / \
                                                            data['rolling_12week_y'] * 100
    data['rolling_12week_quantity'] = pd.rolling_sum(data['y'], window=12, min_periods=12)

    data = data.replace([-np.inf, np.inf], np.nan)
    return data

def monthly_prophet_model_error_calc(_data):

    import pandas as pd
    import numpy as np

    data = _data.copy()

    # Calculate total ACTUAL Quantity
    data['cumsum_quantity'] = data.y.cumsum()

    data['Error_prophet'] = np.subtract(data.y_Prophet, data.y)
    data['Error_Cumsum_prophet'] = data.Error_prophet.cumsum() / data.y.cumsum() * 100

    # Calculate Rolling 6-week and 12-week error for Prophet
    data['rolling_3month_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=3,
                                                         min_periods=3)
    data['rolling_3month_y'] = pd.rolling_sum(data['y'], window=3, min_periods=3)
    data['rolling_3month_percent_error_prophet'] = data['rolling_3month_error_prophet'] / data[
        'rolling_3month_y'] * 100
    data['rolling_3month_quantity'] = pd.rolling_sum(data['y'], window=3, min_periods=3)

    data['rolling_4month_error_prophet'] = pd.rolling_sum(data['Error_prophet'], window=4,
                                                          min_periods=4)
    data['rolling_4month_y'] = pd.rolling_sum(data['y'], window=4, min_periods=4)
    data['rolling_4month_percent_error_prophet'] = data['rolling_4month_error_prophet'] / \
                                                   data['rolling_4month_y'] * 100
    data['rolling_4month_quantity'] = pd.rolling_sum(data['y'], window=4, min_periods=4)

    data = data.replace([-np.inf, np.inf], np.nan)
    return (data)

def monthly_arima_model_error_calc(_data):

    import pandas as pd
    import numpy as np

    data = _data.copy()

    # Calculate total ACTUAL Quantity
    data['cumsum_quantity'] = data.y.cumsum()

    data['Error_arima'] = np.subtract(data.y_ARIMA, data.y)
    data['Error_Cumsum_arima'] = data.Error_arima.cumsum() / data.y.cumsum() * 100

    # Calculate Rolling 6-week and 12-week error for Arima
    data['rolling_3month_error_arima'] = pd.rolling_sum(data['Error_arima'], window=3,
                                                         min_periods=3)
    data['rolling_3month_y'] = pd.rolling_sum(data['y'], window=3, min_periods=3)
    data['rolling_3month_percent_error_arima'] = data['rolling_3month_error_arima'] / data[
        'rolling_3month_y'] * 100
    data['rolling_3month_quantity'] = pd.rolling_sum(data['y'], window=3, min_periods=3)

    data['rolling_4month_error_arima'] = pd.rolling_sum(data['Error_arima'], window=4,
                                                          min_periods=4)
    data['rolling_4month_y'] = pd.rolling_sum(data['y'], window=4, min_periods=4)
    data['rolling_4month_percent_error_arima'] = data['rolling_4month_error_arima'] / \
                                                   data['rolling_4month_y'] * 100
    data['rolling_4month_quantity'] = pd.rolling_sum(data['y'], window=4, min_periods=4)

    data = data.replace([-np.inf, np.inf], np.nan)
    return (data)