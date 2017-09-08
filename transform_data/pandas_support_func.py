
def extract_from_dict(row_elem, **kwargs):
    if(kwargs.get('multi_indexes')==True):
        return [[row_elem.get(index).get(key) for key in row_elem.get(index).keys()] for index in row_elem.keys()]

    return [row_elem.get(row_elem.keys()[0]).get(key) for key in row_elem.get(row_elem.keys()[0]).keys()]


def convert_list_to_pd_df(holidays):
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

import pandas as pd
from pandas import Timestamp
import numpy as np

df2 = pd.DataFrame({ 'A' : 1.,
                         'B' : pd.Timestamp('20130102'),
                         'C' : pd.Series(1,index=list(range(4)),dtype='float32'),
                        'D' : np.array([3] * 4,dtype='int32'),
                         'E' : pd.Categorical(["test","train","test","train"]),
                         'F' : 'foo' })

print df2.to_dict(orient='index')
list_elem = extract_from_dict(df2.to_dict(orient='index'))

print pd.DataFrame.from_dict(data={0: {'A': 1.0, 'C': 1.0, 'B': Timestamp('2013-01-02 00:00:00'), 'E': 'test', 'D': 3, 'F': 'foo'}}, orient='index')



# for elem in list_elem:
#     print type(elem)

x = pd.Series([1, 2, 3, 4, -1, -2, -3, -4])
print "printing X"
print x
b = np.absolute(x)
print "printing b"
print b
print type(b)
#
c = np.nanmax(b)
print "print c"
print c
print type(c)

print np.nanmax(np.absolute(np.array(pd.Series([1, 2, 3, 4, -1, -2, -3, -4]))))