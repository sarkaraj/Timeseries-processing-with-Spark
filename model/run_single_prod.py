# from transform_data.data_transform import *
# from model.ma_outlier import *
from model.weekly_model import *
# # from model.monthly_pydlm import *
# # from model.moving_average import *
# from distributed_grid_search._model_params_set import *
# from distributed_grid_search._pydlm_monthly import *
# # from distributed_grid_search._fbprophet import *
# from distributed_grid_search._pydlm_monthly import *
# # from model.moving_average_monthly import *
from model.monthly_model import *
from model.plt_data import *

# loading libs
import pandas as pd
# import time
import numpy as np
from dateutil import parser
import matplotlib.pylab as plt
# %matplotlib inline
from matplotlib.pylab import rcParams

rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\files\\CONA_Conv_Store_Data\\"

# image save folder
image_dir = "C:\\files\\CONA_Conv_Store_Data\\temp\\monthly_prophet\\images\\CCBF\\modeling_result"

# holidays
holidays = pd.read_table(file_dir + 'holidays.csv', delimiter=',', header=0)
holidays.ds = holidays.ds.apply(parser.parse)
holidays.lower_window = -7
holidays.upper_window = 7

# data transformation to weekly and monthly aggregate
raw_data = pd.read_csv(file_dir + "raw_data_FL_ALL_cutoff_dt_05-11-2017.tsv", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])
cus_no = 500124803
mat_no = 115583

cus = raw_data[raw_data.customernumber == cus_no]
prod = cus[cus.matnr == mat_no]

prod = get_weekly_aggregate(inputDF=prod)

result = weekly_ensm_model(prod= prod, cus_no= cus_no, mat_no= mat_no, holidays= holidays,
                           dir_name = image_dir)

print(result)

# for mat_no in cus.matnr.unique():
#     prod = cus[cus.matnr == mat_no]
#     data_weekly = get_weekly_aggregate(inputDF=prod)
#     data_weekly.dt_week = data_weekly.dt_week.apply(str).apply(parser.parse)
#
#     prod.date = prod.date.apply(str).apply(parser.parse)
#     prod.y = prod.quantity.apply(float)
#     prod = prod.sort_values('date')
#     prod = prod.reset_index(drop=True)
#     plot_raw_data(data=prod, dir_name=image_dir, cus_no=cus_no, mat_no=mat_no)
#
#     prod = data_weekly
#     prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
#     plot_weekly_data(data=prod, dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)
#
#     monthly_data = get_monthly_aggregate_per_product(prod)
#     plot_monthly_data(data=monthly_data, dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)


