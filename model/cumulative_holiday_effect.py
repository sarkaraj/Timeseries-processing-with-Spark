import numpy as np
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
from transform_data.data_transform import get_weekly_aggregate
from model.save_images import *
from transform_data.holidays import *
rcParams['figure.figsize'] = 15, 6

# data load and transform
raw_data_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\H_effect_per_mat\\"

# read raw invoices data
raw_data = pd.read_csv(raw_data_dir + "raw_invoices_2018-07-06.tsv",
                       sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

raw_data_pos_q = raw_data[raw_data['quantity'] >= 0]
raw_data_pos_q = raw_data_pos_q.reset_index(drop=True)

print("raw data:\n")
print(raw_data_pos_q.head())
print("#####################################\n")

###########################################################################
# aggregating cumulative cus data
###########################################################################
#
# cum_raw_data = raw_data_pos_q
# cum_raw_data['customernumber'] = 1
# cum_raw_data['matnr'] = 1
#
# # print(cum_raw_data.head())
#
# cum_w_agg_data = get_weekly_aggregate(inputDF= cum_raw_data)
#
# print("weekly aggregated cumulative data:\n")
# print(cum_w_agg_data.head())
# print("#####################################\n")
#
# cum_w_agg_data = cum_w_agg_data[['dt_week', 'quantity']]
# cum_w_agg_data = cum_w_agg_data.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
#
# cum_w_agg_data.ds = cum_w_agg_data.ds.apply(str).apply(parser.parse)
# cum_w_agg_data.y = cum_w_agg_data.y.apply(float)
# cum_w_agg_data = cum_w_agg_data.sort_values('ds')
# cum_w_agg_data = cum_w_agg_data.reset_index(drop=True)
#
# print("weekly aggregated cumulative data transformed:\n")
# print(cum_w_agg_data.head())
# print("#####################################\n")
# ##########################################################################
#
# # obtaining holidays data
# ##################################################################
# _holidays = get_holidays_dataframe_pd()
# _holidays['week_before'] = _holidays['ds'] + pd.DateOffset(days = -7)
# _holidays['week_after'] = _holidays['ds'] + pd.DateOffset(days = 7)
# print(_holidays.head())
# print('###########################################################\n')
#
#
# prod_holidays = _holidays[_holidays['week_after'] >= min(cum_w_agg_data['ds'])]
# prod_holidays = prod_holidays[prod_holidays['week_after'] <= max(cum_w_agg_data['ds'] + pd.DateOffset(14))]
#
# print("Holidays List:\n")
# print(prod_holidays.tail())
# print("#####################################################\n")
#
# holidays_save_plots(x= cum_w_agg_data.ds, y = cum_w_agg_data.y,
#                     xlable= "Date", ylable= "Quantity",
#                     holidays_count= len(prod_holidays), holiday_start_day_list= prod_holidays['week_before'],
#                     holidays_end_day_list= prod_holidays['week_after'], holiday_name= np.array(prod_holidays.holiday),
#                     title= "Holiday_Effect", dir_name= image_dir, cus_no= "all_cus", mat_no= "all_mat")
###########################################################################################



###########################################################################################
# aggregate per customer
###########################################################################################
# cus_cum_raw_data = raw_data_pos_q
# # cum_raw_data['customernumber'] = 1
# cus_cum_raw_data['matnr'] = 1
#
#
# for cus_no in cus_cum_raw_data['customernumber'].unique():
#     cus = cus_cum_raw_data[cus_cum_raw_data['customernumber'] == cus_no]
#
#     cum_w_agg_data = get_weekly_aggregate(inputDF= cus)
#
#     print("weekly aggregated cumulative data:\n")
#     print(cum_w_agg_data.head())
#     print("#####################################\n")
#
#     cum_w_agg_data = cum_w_agg_data[['dt_week', 'quantity']]
#     cum_w_agg_data = cum_w_agg_data.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
#
#     cum_w_agg_data.ds = cum_w_agg_data.ds.apply(str).apply(parser.parse)
#     cum_w_agg_data.y = cum_w_agg_data.y.apply(float)
#     cum_w_agg_data = cum_w_agg_data.sort_values('ds')
#     cum_w_agg_data = cum_w_agg_data.reset_index(drop=True)
#
#     print("weekly aggregated cumulative data transformed:\n")
#     print(cum_w_agg_data.head())
#     print("#####################################\n")
#     ##########################################################################
#
#     # obtaining holidays data
#     ##################################################################
#     _holidays = get_holidays_dataframe_pd()
#     _holidays['week_before'] = _holidays['ds'] + pd.DateOffset(days = -7)
#     _holidays['week_after'] = _holidays['ds'] + pd.DateOffset(days = 7)
#     print(_holidays.head())
#     print('###########################################################\n')
#
#
#     prod_holidays = _holidays[_holidays['week_after'] >= min(cum_w_agg_data['ds'])]
#     prod_holidays = prod_holidays[prod_holidays['week_after'] <= max(cum_w_agg_data['ds'] + pd.DateOffset(14))]
#
#     print("Holidays List:\n")
#     print(prod_holidays.tail())
#     print("#####################################################\n")
#
#     holidays_save_plots(x= cum_w_agg_data.ds, y = cum_w_agg_data.y,
#                         xlable= "Date", ylable= "Quantity",
#                         holidays_count= len(prod_holidays), holiday_start_day_list= prod_holidays['week_before'],
#                         holidays_end_day_list= prod_holidays['week_after'], holiday_name= np.array(prod_holidays.holiday),
#                         title= "Holiday_Effect", dir_name= image_dir, cus_no= cus_no, mat_no= "all_mat")
###########################################################################################


###########################################################################################
# aggregate per product
###########################################################################################
mat_cum_raw_data = raw_data_pos_q
mat_cum_raw_data['customernumber'] = 1
# cus_cum_raw_data['matnr'] = 1


for mat_no in mat_cum_raw_data['matnr'].unique():
    mat = mat_cum_raw_data[mat_cum_raw_data['matnr'] == mat_no]

    cum_w_agg_data = get_weekly_aggregate(inputDF= mat)

    print("weekly aggregated cumulative data:\n")
    print(cum_w_agg_data.head())
    print("#####################################\n")

    cum_w_agg_data = cum_w_agg_data[['dt_week', 'quantity']]
    cum_w_agg_data = cum_w_agg_data.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

    cum_w_agg_data.ds = cum_w_agg_data.ds.apply(str).apply(parser.parse)
    cum_w_agg_data.y = cum_w_agg_data.y.apply(float)
    cum_w_agg_data = cum_w_agg_data.sort_values('ds')
    cum_w_agg_data = cum_w_agg_data.reset_index(drop=True)

    print("weekly aggregated cumulative data transformed:\n")
    print(cum_w_agg_data.head())
    print("#####################################\n")
    ##########################################################################

    # obtaining holidays data
    ##################################################################
    _holidays = get_holidays_dataframe_pd()
    _holidays['week_before'] = _holidays['ds'] + pd.DateOffset(days = -7)
    _holidays['week_after'] = _holidays['ds'] + pd.DateOffset(days = 7)
    print(_holidays.head())
    print('###########################################################\n')


    prod_holidays = _holidays[_holidays['week_after'] >= min(cum_w_agg_data['ds'])]
    prod_holidays = prod_holidays[prod_holidays['week_after'] <= max(cum_w_agg_data['ds'] + pd.DateOffset(14))]

    print("Holidays List:\n")
    print(prod_holidays.tail())
    print("#####################################################\n")

    holidays_save_plots(x= cum_w_agg_data.ds, y = cum_w_agg_data.y,
                        xlable= "Date", ylable= "Quantity",
                        holidays_count= len(prod_holidays), holiday_start_day_list= prod_holidays['week_before'],
                        holidays_end_day_list= prod_holidays['week_after'], holiday_name= np.array(prod_holidays.holiday),
                        title= "Holiday_Effect", dir_name= image_dir, cus_no= "all_cus", mat_no= mat_no)