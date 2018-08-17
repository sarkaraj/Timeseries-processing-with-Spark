from transform_data.data_transform import *
from model.ma_outlier import *
from distributed_grid_search._sarimax import *
from od_comp.support_func import plot_count_hist, insert_missing_dates, filter_mismatch_dates
from distributed_grid_search._sarimax_monthly import *
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
import time
import os

rcParams['figure.figsize'] = 15, 6

############################
route_id_list = ['CC002189']

start_date = '2018-07-02'
end_date = '2018-07-27'
############################

# data load and transform
file_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

cv_result_dir = "C:\\CONA_CSO\\thadeus_route\\cv_result\\"

order_compare_dir = "C:\\CONA_CSO\\thadeus_route\\compare\\"

vl_dir = "C:\\CONA_CSO\\thadeus_route\\vl\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\temp\\"

order_compare = pd.read_csv(order_compare_dir + "compare_2018-08-09.tsv", sep= "\t", header=None,
                            names=["customernumber", "mat_no", "order_date", "actual_q", "pred_q", "dd_actual",
                                   "dd_pred", "month"])

print("Order compare data:\n")
print(order_compare.head())

order_compare[['order_date']] = order_compare[['order_date']].apply(lambda x: x.astype(str).apply(parser.parse))

###############################################
cv_result = pd.read_csv(cv_result_dir + "cv_result_2018-08-03.tsv",
                          sep= "\t", header= None, names= ["customernumber", "mat_no", "rmse", "mae", "mape", "cum_error", "period_days", "wre_max_6",
                                                           "wre_med_6", "wre_mean_6", "quantity_mean_6", "wre_max_12", "wre_med_12",
                                                           "wre_mean_12", "quantity_mean_12", "wre_max_24", "wre_med_24",
                                                           "wre_mean_24", "quantity_mean_24", "wre_max_48", "wre_med_48",
                                                           "wre_mean_48", "quantity_mean_48", "params", "cat", "mdl_bld_dt"])

cv_result_cat_123 = cv_result.loc[cv_result['cat'].isin(['I', 'II', 'III', 'VII'])]

cv_result_cat_123 = cv_result_cat_123.loc[:,['customernumber', 'mat_no', 'cat']]
# print(cv_result_cat_123.head())

################################################


##############################################
# obtaining customer list for the given route
###############################################
vl = pd.read_csv(vl_dir + "AZ_TCAS_VL.csv",sep=",", header=0, encoding = "ISO-8859-1")

vl_select_route = vl.loc[vl['USERID'].isin(route_id_list)]

customer_list = list(set(vl_select_route['KUNNR'].values))
# print(customer_list)
################################################
# filter compare result
order_comp_select_cust = order_compare.loc[(order_compare['customernumber'].isin(customer_list)) &
                                           ((order_compare['order_date']>= start_date) &
                                            (order_compare['order_date']<= end_date))]
print("raw order date compare data:\n")
print(order_comp_select_cust.head())
################################################
order_comp_select_cust = pd.merge(left= order_comp_select_cust, right= cv_result_cat_123, how = "left",
                                  left_on=["customernumber", 'mat_no'], right_on= ["customernumber", 'mat_no'])

order_comp_select_cust = order_comp_select_cust.loc[order_comp_select_cust['cat'].isin(['I', 'II', 'III', 'VII'])]

print('selected high frequency customer order compare data:\n')
print(order_comp_select_cust.head())

################################################
#orderate basis comparison
od_order_comp = order_comp_select_cust.copy()
od_order_comp['q_diff_abs'] = abs(od_order_comp['actual_q'] - od_order_comp['pred_q'])
od_order_comp['perc_diff_abs'] = abs(od_order_comp['actual_q'] - od_order_comp['pred_q'])/ \
                                 od_order_comp['actual_q']*100
od_order_comp['q_diff'] = od_order_comp['pred_q'] - od_order_comp['actual_q']
od_order_comp['perc_diff'] = (od_order_comp['pred_q'] - od_order_comp['actual_q'])/od_order_comp['actual_q']*100

od_order_comp.drop(['dd_actual', 'dd_pred', 'month'], axis=1, inplace = True)
print('final order date compare data:\n')
print(od_order_comp.head(50))

################################################
# insert zeros for missing order dates for all products
od_order_comp_with_zeros = insert_missing_dates(data= od_order_comp)

od_order_comp_with_zeros_cleaned = filter_mismatch_dates(data= od_order_comp_with_zeros)

# print(od_order_comp_with_zeros_cleaned.head(10))

print("printing oder date data length:\n")
print(len(od_order_comp))

print("printing order dates with zeros length:\n")
print(len(od_order_comp_with_zeros))

print("printing cleaned order dates with zeros length:\n")
print(len(od_order_comp_with_zeros_cleaned))

################################################
# plot_count_hist(data=od_order_comp, field= 'q_diff_abs', title='Histogram of Error Quantity on Order Date Basis',
#                 num_bar=10, image_dir=image_dir)

plot_count_hist(data=od_order_comp_with_zeros_cleaned.loc[(od_order_comp_with_zeros_cleaned['q_diff'] >= -5) &
                                                          (od_order_comp_with_zeros_cleaned['q_diff'] <= 5)],
                field= 'q_diff',
                title='Histogram of Error Quantity on Order Date Basis with zeros(pred-actual)',
                x_label= 'Error Quantity(cs)',
                num_bar=11, x_lim=10.5, image_dir=image_dir)

for i in range(-4,5):
    plot_count_hist(data=od_order_comp_with_zeros_cleaned.loc[(od_order_comp_with_zeros_cleaned['q_diff'] == i)],
                    field='actual_q',
                    title='Histogram actual quantity for error ' + str(i) + "(pred-actual)",
                    x_label= 'Actual Order(cs)',
                    num_bar=10, x_lim=10.5, image_dir=image_dir)

plot_count_hist(data=od_order_comp_with_zeros_cleaned, field= 'q_diff_abs',
                title='Histogram of Abs Error Quantity on Order Date Basis with zeros', x_label= 'Error Quantity(cs)',
                num_bar=10, x_lim=10.5, image_dir=image_dir)

###################################################################################################################
# weekly aggregate basis comparison
###################################################################################################################
# w_agg_order_comp = od_order_comp.groupby(['customernumber', 'mat_no',
#                                           pd.Grouper(key='order_date', freq='W-MON', closed = 'left',
#                                                      label = 'left')])[['actual_q', 'pred_q']].sum()\
#     .reset_index().sort_values('order_date')
#
# w_agg_order_comp['q_diff_abs'] = abs(w_agg_order_comp['actual_q']- w_agg_order_comp['pred_q'])
# print('weekly agg compare data length:\n')
# print(len(w_agg_order_comp))
#
# w_agg_order_comp_with_zeros = insert_missing_dates(data= w_agg_order_comp)
# print('weekly agg compare data with zeros length:\n')
# print(len(w_agg_order_comp_with_zeros))
#
# w_agg_order_comp_with_zeros_cleaned = filter_mismatch_dates(data= w_agg_order_comp_with_zeros)
# print('weekly agg compare cleaned data length:\n')
# print(len(w_agg_order_comp_with_zeros_cleaned))
#
# # plot_count_hist(data=od_order_comp, field= 'q_diff_abs', title='Histogram of Error Quantity on Order Date Basis',
# #                 num_bar=10, image_dir=image_dir)
#
# plot_count_hist(data=w_agg_order_comp_with_zeros_cleaned, field= 'q_diff_abs',
#                 title='Histogram of Error Quantity on weekly Basis with zeros', x_label= 'Error Quantity(cs)',
#                 num_bar=10, x_lim=10.5, image_dir=image_dir)

#################################################












