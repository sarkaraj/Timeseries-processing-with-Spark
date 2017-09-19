from transform_data.data_transform import *
from model.ma_outlier import *
from model.weekly_model import *
from model.weekly_pydlm import *
from model.moving_average import *

# loading libs
import pandas as pd
import numpy as np
from dateutil import parser
import matplotlib.pylab as plt
# %matplotlib inline
from matplotlib.pylab import rcParams

rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\files\\CONA_Conv_Store_Data\\"

# image save folder
image_dir = "C:\\files\\CONA_Conv_Store_Data\\temp"

# holidays
holidays = pd.read_table(file_dir + 'holidays.csv', delimiter=',', header=0)
holidays.ds = holidays.ds.apply(parser.parse)
holidays.lower_window = -7
holidays.upper_window = 7

# data transformation to weekly and monthly aggregate
raw_data = pd.read_csv(file_dir + "skywaymart_90.txt", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])
data_weekly = get_weekly_aggregate(inputDF=raw_data)
data_weekly.dt_week = data_weekly.dt_week.apply(str).apply(parser.parse)
# data_weekly.head()

# data_monthly = get_monthly_aggregate(inputDF=raw_data)
# data_monthly.dt_week = data_monthly.dt_week.apply(str).apply(parser.parse)
# print data_monthly.head()

# single prod data
cus_no = str(500064458)
mat_no = 119826
cus = data_weekly[data_weekly.customernumber == cus_no]

prod = cus[cus.matnr == mat_no]

prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
prod = prod[['ds', 'y']]
prod = prod.sort_values('ds')
prod = prod.reset_index(drop=True)

prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

# prod = ma_replace_outlier(data = prod,n_pass=3,aggressive= True)

# plt.plot(prod.ds,prod.y)
# plt.show()

# run weekly model

# output = weekly_ensm_model(prod=prod, cus_no=cus_no, mat_no=mat_no,
#                            test_points=6)

# weekly_pydlm_model(prod=prod, cus_no=cus_no, mat_no=mat_no)

(output_error, pred) = moving_average_model(prod = prod, cus_no = cus_no, mat_no= mat_no,weekly_data = False,
                         weekly_window= 6, monthly_window = 3)
print (output_error.head())
