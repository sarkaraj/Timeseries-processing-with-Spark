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
image_dir = "C:\\files\\CONA_Conv_Store_Data\\temp\\monthly_prophet\\just_images\\"

# holidays
holidays = pd.read_table(file_dir + 'holidays.csv', delimiter=',', header=0)
holidays.ds = holidays.ds.apply(parser.parse)
holidays.lower_window = -7
holidays.upper_window = 7

# data transformation to weekly and monthly aggregate
raw_data = pd.read_csv(file_dir + "raw_invoice_data_sample_FL_200_cutoff_date_05-11-2017.tsv", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])
cus_no = 500128741
mat_no = 103029
cus = raw_data[raw_data.customernumber == cus_no]

prod = cus[cus.matnr == mat_no]
data_weekly = get_weekly_aggregate(inputDF=prod)
data_weekly.dt_week = data_weekly.dt_week.apply(str).apply(parser.parse)
# data_weekly.head()

# data_monthly = get_monthly_aggregate(inputDF=raw_data)
# data_monthly.dt_week = data_monthly.dt_week.apply(str).apply(parser.parse)
# print data_monthly.head()

# single prod data
# cus_no = 500141055
# mat_no = 103029
# cus = raw_data[raw_data.customernumber == cus_no]
#
# prod = cus[cus.matnr == mat_no]

prod.date = prod.date.apply(str).apply(parser.parse)
prod.y = prod.quantity.apply(float)
prod = prod.sort_values('date')
prod = prod.reset_index(drop=True)
plot_raw_data(data=prod, dir_name=image_dir, cus_no=cus_no, mat_no=mat_no)


prod = data_weekly

prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

plot_weekly_data(data=prod, dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)

monthly_data = get_monthly_aggregate_per_product(prod)

plot_monthly_data(data=monthly_data, dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)

# print(prod.head())

# def weekly_ensm_model(prod, cus_no, mat_no, min_train_days=731, test_points=2, holidays=get_holidays_dataframe_pd(),
#                       **kwargs)
# output = weekly_ensm_model(prod=prod, cus_no=cus_no,mat_no=mat_no, holidays= holidays,dir_name= image_dir)
#
# print(output)

# print(data_weekly.head())

# import time
# start_time = time.time()
#
# for elem in generate_all_param_combo_pydlm_monthly():
#     run_pydlm_monthly(cus_no=cus_no, mat_no=mat_no,prod=prod,test_points = 2, param = elem)
#     # print(output[1][1])
#
# print("--- %s seconds ---" % (time.time() - start_time))

# prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
# prod = prod[['ds', 'y']]
# prod = prod.sort_values('ds')
# prod = prod.reset_index(drop=True)
#
# prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

# prod = ma_replace_outlier(data = prod,n_pass=3,aggressive= True)

# plt.plot(prod.ds,prod.y)
# plt.show()

# run weekly model

# output = monthly_prophet_model(prod=prod, cus_no=cus_no, mat_no=mat_no,
#                            test_points=1)



# weekly_pydlm_model(prod=prod, cus_no=cus_no, mat_no=mat_no)
#
# (output_error, pred) = moving_average_model(prod = prod, cus_no = cus_no, mat_no= mat_no,weekly_data = False,
#                          weekly_window= 6, monthly_window = 3)
# print (output_error.head())
