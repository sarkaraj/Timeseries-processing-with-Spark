# import pandas as pd
from transform_data.data_transform import *

holidays_list = """ds,holiday,lower_window,upper_window
1/2/2012,New Year Day,15,1
5/28/2012,Memorial Day,15,1
7/4/2012,Independence Day,15,1
9/3/2012,Labor Day,15,1
11/22/2012,Thanksgiving Day,15,1
12/25/2012,Christmas Day,15,1
1/1/2013,New Year Day,15,1
5/27/2013,Memorial Day,15,1
7/4/2013,Independence Day,15,1
9/2/2013,Labor Day,15,1
11/28/2013,Thanksgiving Day,15,1
12/25/2013,Christmas Day,15,1
1/1/2014,New Year Day,15,1
5/26/2014,Memorial Day,15,1
7/4/2014,Independence Day,15,1
9/1/2014,Labor Day,15,1
11/27/2014,Thanksgiving Day,15,1
12/25/2014,Christmas Day,15,1
1/1/2015,New Year Day,15,1
5/25/2015,Memorial Day,15,1
7/3/2015,Independence Day,15,1
9/7/2015,Labor Day,15,1
11/26/2015,Thanksgiving Day,15,1
12/25/2015,Christmas Day,15,1
1/1/2016,New Year Day,15,1
5/30/2016,Memorial Day,15,1
7/4/2016,Independence Day,15,1
9/5/2016,Labor Day,15,1
11/24/2016,Thanksgiving Day,15,1
12/25/2016,Christmas Day,15,1
1/2/2017,New Year Day,15,1
5/29/2017,Memorial Day,15,1
7/4/2017,Independence Day,15,1
9/4/2017,Labor Day,15,1
11/23/2017,Thanksgiving Day,15,1
12/25/2017,Christmas Day,15,1
1/1/2018,New Year Day,15,1
5/28/2018,Memorial Day,15,1
7/4/2018,Independence Day,15,1
9/3/2018,Labor Day,15,1
11/22/2018,Thanksgiving Day,15,1
12/25/2018,Christmas Day,15,1
1/1/2019,New Year Day,15,1
5/27/2019,Memorial Day,15,1
7/4/2019,Independence Day,15,1
9/2/2019,Labor Day,15,1
11/28/2019,Thanksgiving Day,15,1
12/25/2019,Christmas Day,15,1
1/1/2020,New Year Day,15,1
5/25/2020,Memorial Day,15,1
7/3/2020,Independence Day,15,1
9/7/2020,Labor Day,15,1
11/26/2020,Thanksgiving Day,15,1
12/25/2020,Christmas Day,15,1"""


def get_holidays_dataframe_pd(_list=holidays_list):
    import pandas as pd
    from dateutil import parser

    lines = _list.split("\n")[1:]
    dict_list = [{index: {'ds': line.split(",")[0], 'holiday': line.split(",")[1], 'lower_window': line.split(",")[2],
                          'upper_window': line.split(",")[3]}} for index, line in enumerate(lines)]

    holidays_dict = {}
    [holidays_dict.update(dict_line) for dict_line in dict_list]

    holidays = pd.DataFrame.from_dict(data=holidays_dict, orient='index')

    holidays.ds = holidays.ds.apply(parser.parse)
    holidays.lower_window = -7
    holidays.upper_window = 7

    return holidays

def get_holidays_sarimax(_holiday_df = get_holidays_dataframe_pd(_list=holidays_list)):
    import pandas as pd
    _holidays = _holiday_df
    _holidays['week_before'] = _holidays['ds'] + pd.DateOffset(days = -7)
    _holidays['week_after'] = _holidays['ds'] + pd.DateOffset(days = 7)

    _holidays_cw = _holidays[['ds', 'holiday']].reset_index(drop= True)
    _holidays_wb = _holidays[['week_before', 'holiday']].rename(columns={'week_before': 'ds'})
    _holidays_wb['holiday'] = _holidays_wb['holiday'].apply(lambda x: "Pre " + x)
    _holidays_wa = _holidays[['week_after', 'holiday']].rename(columns = {'week_after': 'ds'})
    _holidays_wa['holiday'] = _holidays_wa['holiday'].apply(lambda x: "Post " + x)

    _holidays_window = pd.concat([_holidays_cw,_holidays_wb, _holidays_wa], axis= 0, ignore_index=True, verify_integrity= True)
    _holidays_window = _holidays_window.sort_values(by = ['ds', 'holiday'])
    _holidays_window = _holidays_window.reset_index(drop=True)
    return _holidays_window

def generate_sarimax_holiday_input_data(ts_date_df, _holiday = get_holidays_sarimax()):
    import datetime
    import pandas as pd
    ts_date_df['year_week'] = ts_date_df['ds'].apply(str).apply(lambda x: x.split(" ")[0])\
        .apply(lambda x: string_to_gregorian(x)).apply(lambda x: str(
        x.isocalendar()[0]) + "-" + str(x.isocalendar()[1]))
    _holiday['year_week'] = _holiday['ds'].apply(str).apply(lambda x: x.split(" ")[0]) \
        .apply(lambda x: string_to_gregorian(x)).apply(
        lambda x: str(x.isocalendar()[0]) + "-" + str(x.isocalendar()[1]))
    _holidays_weeks = _holiday.drop('ds', axis = 1)
    _holidays_weeks.holiday = _holidays_weeks.holiday.astype('category')
    _holidays_dummy = pd.get_dummies(_holidays_weeks.holiday)
    _holidays_weeks_dummy = pd.concat([_holidays_weeks.drop('holiday', axis = 1), _holidays_dummy], axis= 1)
    _holidays_weeks_agg = _holidays_weeks_dummy.groupby(['year_week'], as_index=False).sum()
    sarimax_holiday_input = pd.merge(ts_date_df, _holidays_weeks_agg, how='left', left_on=['year_week'],
                                     right_on=['year_week'])
    return sarimax_holiday_input.drop('year_week', axis = 1)

if __name__ == "__main__":
    import pandas as pd
    from dateutil import parser
    # print(get_holidays_dataframe_pd(_list=holidays_list))
    # df = get_holidays_dataframe_pd(_list=holidays_list)
    # print(df["holiday"].unique())

    df = get_holidays_sarimax()

    t = {'ds': ["2018-01-02", "2018-01-09"], 'quantity': [3, 4]}
    ts = pd.DataFrame(data=t)
    ts.ds = ts.ds.apply(str).apply(parser.parse)
    ts.quantity = ts.quantity.apply(float)

    ts_data = generate_sarimax_holiday_input_data(ts_date_df= ts)
    # df_new = pd.DataFrame(df)
    # df_new = df_new.sort_values['ds']
    print(ts_data.set_index('ds', drop=True).values.astype('float'))
    # print((df.sort_values(by=['ds','holiday'])))
    # print(df.tail(50))