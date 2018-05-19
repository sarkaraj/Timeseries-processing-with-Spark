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


if __name__ == "__main__":
    # print(get_holidays_dataframe_pd(_list=holidays_list))
    df = get_holidays_dataframe_pd(_list=holidays_list)
    print(df["holiday"].unique())