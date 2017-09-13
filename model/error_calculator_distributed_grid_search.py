def weekly_arima_error_calc(data):

    import pandas as pd
    import numpy as np

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

    data['rolling_12week_error_arima'] = pd.rolling_sum(data['Error_arima'], window=12,
                                                                 min_periods=12)
    data['rolling_12week_y'] = pd.rolling_sum(data['y'], window=12, min_periods=12)
    data['rolling_12week_percent_error_arima'] = data['rolling_12week_error_arima'] / data[
        'rolling_12week_y'] * 100

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

    return data
