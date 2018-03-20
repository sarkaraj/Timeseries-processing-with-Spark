# changes to be made for python 2.7
# import izip and change it two places
# .keys and .values need to be changed

import collections
from itertools import count
import pandas as pd
import numpy as np
import os
import matplotlib.pylab as plt
# import itertools
# import warnings
# import statsmodels.api as sm
# from dateutil import parser
# import datetime as dt



def moving_average(data, window_size):
    """ Computes moving average using discrete linear convolution of two one dimensional sequences.
    Args:
    -----
            data (pandas.Series): independent variable
            window_size (int): rolling window size

    Returns:
    --------
            ndarray of linear convolution

    References:
    ------------
    [1] Wikipedia, "Convolution", http://en.wikipedia.org/wiki/Convolution.
    [2] API Reference: https://docs.scipy.org/doc/numpy/reference/generated/numpy.convolve.html

    """
    import numpy as np
    window = np.ones(int(window_size)) / float(window_size)
    if (window_size % 2 == 0):  # even
        beg = int(window_size/2)
        end = int(window_size/2) - 1
        mean_beg = np.array([np.mean(data[:window_size])] * beg)
        mean_end = np.array([np.mean(data[:window_size])] * end)
    else: #odd
        beg = int((window_size - 1) / 2)
        end = int((window_size - 1) / 2)
        mean_beg = np.array([np.mean(data[:window_size])] * beg)
        mean_end = np.array([np.mean(data[:window_size])] * end)
    data = np.concatenate([mean_beg, np.array(data), mean_end])

    return np.convolve(data, window, 'valid')


def explain_anomalies(y, window_size, sigma=1.0):
    """ Helps in exploring the anomalies using stationary standard deviation
    Args:
    -----
        y (pandas.Series): independent variable
        window_size (int): rolling window size
        sigma (int): value for standard deviation

    Returns:
    --------
        a dict (dict of 'standard_deviation': int, 'anomalies_dict': (index: value))
        containing information about the points indentified as anomalies

    """
    import numpy as np
    from collections import OrderedDict
    from itertools import count

    avg = moving_average(y, window_size).tolist()
    residual = y - avg
    # Calculate the variation in the distribution of the residual
    std = np.std(residual)
    temp = {'standard_deviation': round(std, 3), 'anomalies_dict': collections.OrderedDict(
        [(index, y_i) for index, y_i, avg_i in zip(count(), y, avg) if
         (y_i > avg_i + (sigma * std)) | (y_i < avg_i - (sigma * std))])}

    return temp


def explain_anomalies_rolling_std(y, window_size, sigma=1.0):
    """ Helps in exploring the anamolies using rolling standard deviation
    Args:
    -----
        y (pandas.Series): independent variable
        window_size (int): rolling window size
        sigma (int): value for standard deviation

    Returns:
    --------
        a dict (dict of 'standard_deviation': int, 'anomalies_dict': (index: value))
        containing information about the points indentified as anomalies
    """
    avg = moving_average(y, window_size)
    avg_list = avg.tolist()
    residual = y - avg
    # Calculate the variation in the distribution of the residual
    testing_std = pd.rolling_std(residual, window_size)
    testing_std_as_df = pd.DataFrame(testing_std)
    rolling_std = testing_std_as_df.replace(np.nan,
                                            testing_std_as_df.ix[window_size - 1]).round(3).iloc[:, 0].tolist()
    std = np.std(residual)
    return {'stationary standard_deviation': round(std, 3),
            'anomalies_dict': collections.OrderedDict([(index, y_i)
                                                       for index, y_i, avg_i, rs_i in zip(count(),
                                                                                          y, avg_list, rolling_std)
                                                       if (y_i > avg_i + (sigma * rs_i)) | (
                                                           y_i < avg_i - (sigma * rs_i))])}


# This function is repsonsible for displaying how the function performs on the given dataset.
def get_anomaly_index(x, y, window_size, sigma_value=1,
                      text_xlabel="X Axis", text_ylabel="Y Axis",
                      applying_rolling_std=False, **kwargs):
    """ Helps in generating the plot and flagging the anamolies.
        Supports both moving and stationary standard deviation. Use the 'applying_rolling_std' to switch
        between the two.
    Args:
    -----
        x (pandas.Series): dependent variable
        y (pandas.Series): independent variable
        window_size (int): rolling window size
        sigma_value (int): value for standard deviation
        text_xlabel (str): label for annotating the X Axis
        text_ylabel (str): label for annotatin the Y Axis
        step (int): just for saving images
        applying_rolling_std (boolean): True/False for using rolling vs stationary standard deviation
    """
    y_av = moving_average(y, window_size)

    events = {}
    if applying_rolling_std:
        events = explain_anomalies_rolling_std(y, window_size=window_size, sigma=sigma_value)
    else:
        events = explain_anomalies(y, window_size=window_size, sigma=sigma_value)

    x_anomaly = np.fromiter(events['anomalies_dict'].keys(), dtype=int, count=len(events['anomalies_dict']))
    y_anomaly = np.fromiter(events['anomalies_dict'].values(), dtype=float,
                            count=len(events['anomalies_dict']))

    # plot outlier
    if set(['dir_name', 'step', 'cus_no', 'mat_no']) <= set(kwargs.keys()):
        dir_name = kwargs.get('dir_name')
        step = kwargs.get('step')
        cus_no = kwargs.get('cus_no')
        mat_no = kwargs.get('mat_no')

        fig = plt.figure(figsize=(15, 8))
        plt.plot(x, y, "k.")
        plt.plot(x, y_av, color='green')
        plt.xlim(0, len(x))
        plt.xlabel(text_xlabel)
        plt.ylabel(text_ylabel)
        plt.plot(x_anomaly, y_anomaly, "r*", markersize=12)
        plt.grid(True)

        # plt.show()

        save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) +
                                 "_" + "MA_outlier" + "_step_" + str(step) + ".png")
        plt.savefig(save_file, bbox_inches='tight')
        plt.close(fig)

    return (x_anomaly)


def ma_replace_outlier(data, n_pass=2, aggressive=True, window_size = 12, sigma = 2, **kwargs):
    """ Used for Moving Average Based outlier removal

    Args:
        pass: number of passes to remove outlier, max = 3
        data: data.ds, data.y
        define cus_no, mat_no, dir_name for image saving. All three needs to be provided necessarily
        for sigma >= 3 aggressive = true/false are same
        max sigma = 3

    """

    if aggressive == True:
        n = 1
        while (n <= n_pass):
            events = explain_anomalies(y=data.y, window_size=window_size, sigma=sigma)
            if set(['dir_name', 'cus_no', 'mat_no']) <= set(kwargs.keys()):
                dir_name = kwargs.get('dir_name')
                cus_no = kwargs.get('cus_no')
                mat_no = kwargs.get('mat_no')
                outlier_index = get_anomaly_index(data.index, y=data.y, window_size=window_size, sigma_value=sigma,
                                                  text_xlabel="Month", text_ylabel="Quantity",
                                                  applying_rolling_std=False
                                                  , dir_name=dir_name, step=n,
                                                  cus_no=cus_no, mat_no=mat_no)
            else:
                outlier_index = get_anomaly_index(data.index, y=data.y, window_size=window_size, sigma_value=sigma,
                                                  text_xlabel="Month", text_ylabel="Quantity",
                                                  applying_rolling_std=False)

            data = data.set_value(outlier_index, 'y', None)
            # plt.plot(data.ds, data.y)
            # plt.show()

            data['ma'] = pd.Series(data.y).rolling(window=window_size, min_periods=1).mean()
            # print(data.iloc[outlier_index, :])

            data.loc[data['y'].isnull(), 'y'] = data['ma']
            # print(data.iloc[outlier_index, :])

            data = data.drop('ma', 1)
            # plt.plot(data.ds, data.y)
            n = n + 1

    if aggressive == False:
        n = 1
        while (n <= n_pass - 1):
            events = explain_anomalies(y=data.y, window_size=window_size, sigma=sigma)
            if set(['dir_name', 'cus_no', 'mat_no']) <= set(kwargs.keys()):
                dir_name = kwargs.get('dir_name')
                cus_no = kwargs.get('cus_no')
                mat_no = kwargs.get('mat_no')
                outlier_index = get_anomaly_index(data.index, y=data.y, window_size=window_size, sigma_value=sigma,
                                                  text_xlabel="Month", text_ylabel="Quantity",
                                                  applying_rolling_std=False
                                                  , dir_name=dir_name, step=n,
                                                  cus_no=cus_no, mat_no=mat_no)
            else:
                outlier_index = get_anomaly_index(data.index, y=data.y, window_size=window_size, sigma_value=sigma,
                                                  text_xlabel="Month", text_ylabel="Quantity",
                                                  applying_rolling_std=False)

            data = data.set_value(outlier_index, 'y', None)
            # plt.plot(data.ds, data.y)
            # plt.show()

            data['ma'] = pd.Series(data.y).rolling(window=window_size, min_periods=1).mean()
            # print(data.iloc[outlier_index, :])

            data.loc[data['y'].isnull(), 'y'] = data['ma']
            # print(data.iloc[outlier_index, :])

            data = data.drop('ma', 1)
            # plt.plot(data.ds, data.y)
            n = n + 1

        events = explain_anomalies(y=data.y, window_size=window_size, sigma=3)
        if set(['dir_name', 'cus_no', 'mat_no']) <= set(kwargs.keys()):
            dir_name = kwargs.get('dir_name')
            cus_no = kwargs.get('cus_no')
            mat_no = kwargs.get('mat_no')
            outlier_index = get_anomaly_index(data.index, y=data.y, window_size=window_size, sigma_value=3,
                                              text_xlabel="Month", text_ylabel="Quantity",
                                              applying_rolling_std=False
                                              ,dir_name=dir_name, step=n,
                                              cus_no=cus_no, mat_no=mat_no)
        else:
            outlier_index = get_anomaly_index(data.index, y=data.y, window_size=window_size, sigma_value=3,
                                              text_xlabel="Month", text_ylabel="Quantity",
                                              applying_rolling_std=False)

        data = data.set_value(outlier_index, 'y', None)
        # plt.plot(data.ds, data.y)
        # plt.show()

        data['ma'] = pd.Series(data.y).rolling(window=window_size, min_periods=1).mean()
        # print(data.iloc[outlier_index, :])

        data.loc[data['y'].isnull(), 'y'] = data['ma']
        # print(data.iloc[outlier_index, :])

        data = data.drop('ma', 1)
        # plt.plot(data.ds, data.y)

    return (data)
