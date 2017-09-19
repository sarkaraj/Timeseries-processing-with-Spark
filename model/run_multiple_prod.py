from model.weekly_model import *
from model.monthly_model import *
from transform_data.data_transform import *
from model.moving_average import *

import numpy as np
import pandas as pd
from dateutil import parser
import datetime as dt
import matplotlib.pylab as plt
# %matplotlib inline
from matplotlib.pylab import rcParams

rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\files\\CONA_Conv_Store_Data\\"

# image save folder
image_dir = "C:\\files\\CONA_Conv_Store_Data\\temp\\monthly_prophet\\all-25-between-30-60\\"

# holidays
holidays = pd.read_table(file_dir + 'holidays.csv', delimiter=',', header=0)
holidays.ds = holidays.ds.apply(parser.parse)
holidays.lower_window = -7
holidays.upper_window = 7
# holidays.head(6)

# data transformation to weekly and monthly aggregate
raw_data = pd.read_csv(file_dir + "25_C005_between_30_60.tsv", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])
data_weekly = get_weekly_aggregate(inputDF=raw_data)
data_weekly.dt_week = data_weekly.dt_week.apply(str).apply(parser.parse)
# data_weekly.head()

# data_monthly = get_monthly_aggregate(inputDF=raw_data)
# data_monthly.dt_week = data_monthly.dt_week.apply(str).apply(parser.parse)
# print data_monthly.head()

# loop to run for all product, all customer
final_data_df = pd.DataFrame()
for cus_no in data_weekly.customernumber.unique():
    cus = data_weekly[data_weekly.customernumber == cus_no]
    for mat_no in cus.matnr.unique():
        prod = cus[cus.matnr == mat_no]

        days = (
        max(prod.dt_week.apply(str).apply(parser.parse)) - min(prod.dt_week.apply(str).apply(parser.parse))).days
        if days > 830:
            # weekly_model_image_saver(prod, cus_no, mat_no, dir_name, holidays, min_train_days=731, test_points=2)
            # prod_output = weekly_ensm_model(prod=prod, cus_no=cus_no,
            #                                 mat_no=mat_no, dir_name=image_dir)

            # monthly_prophet_model(prod, cus_no, mat_no, dir_name, min_train_days=731, test_points=1)
            # prod_output = monthly_prophet_model(prod = prod , cus_no = cus_no, mat_no = mat_no)
            (prod_output, pred) = moving_average_model(prod=prod, cus_no=cus_no, mat_no=mat_no, weekly_data=False,
                                                        weekly_window=6, monthly_window=3)

            final_data_df = pd.concat([final_data_df, prod_output], axis=0)

# final_data_df.to_csv(image_dir + "error.csv", sep=',', header=True)

print (final_data_df)
