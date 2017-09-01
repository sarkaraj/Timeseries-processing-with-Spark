from data_transformation import *


def convertListToPdDF(holidays):
    import pandas as pd
    import numpy as np
    from dateutil import parser

    ds = []
    holiday = []
    lower_window = []
    upper_window = []

    for elem in holidays:
        ds.append(elem.ds)
        holiday.append(elem.holiday)
        lower_window.append(elem.lower_window)
        upper_window.append(elem.upper_window)

    ds = np.array(ds)
    holiday = np.array(holiday)
    lower_window = np.array(lower_window)
    upper_window = np.array(upper_window)

    ds = pd.Series(ds)
    holiday = pd.Series(holiday)
    lower_window = pd.Series(lower_window)
    upper_window = pd.Series(upper_window)

    holidays = pd.concat([ds, holiday, lower_window, upper_window], axis=1)
    holidays.columns = ['ds', 'holiday', 'lower_window', 'upper_window']
    # print holidays

    holidays.ds = holidays.ds.apply(parser.parse)
    holidays.lower_window = -7
    holidays.upper_window = 7

    # print holidays

    return holidays

def makeDataframe(row_object):
    import sys
    sys.path.append('/home/SSHAdmin/.local/lib/python2.7/site-packages/')
    sys.path.append('/home/SSHAdmin/anaconda/lib/python2.7/site-packages/')

    import pandas as pd
    from fbprophet import Prophet
    from pyculiarity import detect_ts

    customernumber = row_object.customernumber
    matnr = row_object.matnr
    pdt_freq_annual = row_object.pdt_freq_annual

    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = pd.DataFrame(data_array, columns=['date', 'quantity', 'q_indep_p'])
    data_pd_df['customernumber'] = customernumber
    data_pd_df['matnr'] = matnr
    data_pd_df['quantity'] = data_pd_df['quantity'].map(float)
    data_pd_df['q_indep_p'] = data_pd_df['q_indep_p'].map(float)

    data_pd_df = data_pd_df.sort(['date'], ascending=True)

    if (pdt_freq_annual >= 12 and pdt_freq_annual < 52):
        data_pd_df = get_monthly_aggregate(data_pd_df)
        # model = some_func_monthly(data_pd_df)
        # return (customernumber, matnr, model, <array of images>)
    elif (pdt_freq_annual > 52):
        data_pd_df = get_weekly_aggregate(data_pd_df)
    else:
        # data_pd_df = pd.DataFrame()
        pass

    return data_pd_df


test_data_rdd = test_data.map(lambda x: makeDataframe(x))