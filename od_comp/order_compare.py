from transform_data.data_transform import *
from model.ma_outlier import *
from distributed_grid_search._sarimax import *
from od_comp.support_func import plot_count_hist, insert_missing_dates, filter_mismatch_dates, perc_diff_bucket, persist_count
from distributed_grid_search._sarimax_monthly import *
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
import time
import os
import shutil

rcParams['figure.figsize'] = 15, 6

############################
route_id_list = ['CC002189', 'CC019018']  #CC019018 #CC002189

start_date = '2016-07-04'
end_date = '2019-09-04'

od_comp_file = "order_comparison_with_decimals_and_capacities_05-09-2018.tsv"  # "order_comparison_04-07-2018_to_05-09-2018_withzeros.tsv" #"order_comparison_production_04-07-2018_to_05-09-2018_withzeros_15pack.csv"#"order_comparison_with_decimals_and_capacities_05-09-2018.tsv"
sep = "\t"
cv_result_file = "cvResult_05-09-2018.tsv"
sub_file_name = "_result_new\\"

logic_new = True

insert_zeros = False
############################

# data load and transform
file_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

cv_result_dir = "C:\\CONA_CSO\\thadeus_route\\cv_result\\"

order_compare_dir = "C:\\CONA_CSO\\thadeus_route\\compare\\"

vl_dir = "C:\\CONA_CSO\\thadeus_route\\vl\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\order_compare\\"

if logic_new == True:
    order_compare = pd.read_csv(order_compare_dir + od_comp_file, sep=sep,
                                header=None,
                                names=["customernumber", "mat_no", "order_date", "actual_q", "pred_q", "dd_actual",
                                       "dd_pred", "month", "pred_dec", "cap", "prod_desc", "fridge"])
else:
    order_compare = pd.read_csv(order_compare_dir + od_comp_file, sep=sep,
                                header=1,
                                names=["customernumber", "mat_no", "order_date", "actual_q", "pred_q", "dd_actual",
                                       "dd_pred", "month"])

print("##########--Order compare data--##############\n")
print(order_compare.head())
print("\n############################################")


order_compare[['order_date']] = order_compare[['order_date']].apply(lambda x: x.astype(str).apply(parser.parse))

###############################################
cv_result = pd.read_csv(cv_result_dir + cv_result_file,
                        sep="\t", header=None,
                        names=["customernumber", "mat_no", "rmse", "mae", "mape", "cum_error", "period_days",
                               "wre_max_6",
                               "wre_med_6", "wre_mean_6", "quantity_mean_6", "wre_max_12", "wre_med_12",
                               "wre_mean_12", "quantity_mean_12", "wre_max_24", "wre_med_24",
                               "wre_mean_24", "quantity_mean_24", "wre_max_48", "wre_med_48",
                               "wre_mean_48", "quantity_mean_48", "cat", "mdl_bld_dt"])

cv_result = cv_result.loc[:, ['customernumber', 'mat_no', 'cat']]
################################################

##############################################
# obtaining customer list for the given route
###############################################
vl = pd.read_csv(vl_dir + "AZ_TCAS_VL.csv", sep=",", header=0, encoding="ISO-8859-1")
vl_select_route = vl.loc[vl['USERID'].isin(route_id_list)]
customer_list = list(set(vl_select_route['KUNNR'].values))
# print(customer_list)

################################################
# filter compare result
order_comp_select_cust = order_compare.loc[(order_compare['customernumber'].isin(customer_list)) &
                                           ((order_compare['order_date'] >= start_date) &
                                            (order_compare['order_date'] <= end_date))]

# print("raw order date compare data:\n")
# print(order_comp_select_cust.head())

# ################################################
# # twice a week customer count analysis
# ################################################
unique_cus_od_list = order_comp_select_cust.groupby(['customernumber'])['order_date'].unique().rename(
    'order_date').reset_index()
s = unique_cus_od_list.apply(lambda x: pd.Series(x['order_date']), axis=1).stack().reset_index(level=1, drop=True)
s.name = 'order_date'
unique_cus_od_list = unique_cus_od_list.drop('order_date', axis=1).join(s).sort_values(
    ['customernumber', 'order_date']).reset_index(drop=True)

unique_days_diff_cus = unique_cus_od_list.groupby(['customernumber']).apply(
    lambda x: (x['order_date'] - x['order_date'].shift(-1))).rename('days_diff').reset_index()
unique_days_diff_cus['days_diff'] = abs(unique_days_diff_cus['days_diff'].apply(lambda x: x.days)).apply(str)

unique_mode_days_cus = unique_days_diff_cus.drop('level_1', axis=1).groupby(['customernumber']).agg(
    lambda x: x.value_counts().index[0]).reset_index()
unique_mode_days_cus['days_diff'] = unique_mode_days_cus['days_diff'].apply(lambda x: float(x))
twice_a_week_cus = unique_mode_days_cus.loc[unique_mode_days_cus['days_diff'] < 7]

# print(twice_a_week_cus['customernumber'].unique())
print("\n#############--Customer Type Analysis--##############")

print("\ntotal customer list: " + str(len(unique_mode_days_cus)))

print("\ntotal twice a week customer list: " + str(len(twice_a_week_cus)))

print("\npercentage of twice a week customer: " + str(len(twice_a_week_cus) / len(unique_mode_days_cus)))

print("\n########################################################")

##################################################
# insert zeros for missing products and remove mismatch delivery dates between orders
##################################################
if insert_zeros == True:
    # insert zeros for missing order dates for all products
    order_comp_with_zeros = insert_missing_dates(data=order_comp_select_cust)
    # print(order_comp_with_zeros.head())
    # print(order_comp_with_zeros.info())

    order_comp_with_zeros_cleaned = filter_mismatch_dates(data=order_comp_with_zeros)
    # print(order_comp_with_zeros_cleaned.head())
    # print(order_comp_with_zeros_cleaned.info())

else:
    order_comp_with_zeros_cleaned = filter_mismatch_dates(data=order_comp_select_cust)
    # print(order_comp_with_zeros_cleaned.head())
    # print(order_comp_with_zeros_cleaned.info())

##################################################
print("\nprinting oder date data length: " + str(len(order_comp_select_cust)))

if insert_zeros == True:
    print("\nprinting order dates with zeros length: " + str(len(order_comp_with_zeros)))

print("\nprinting cleaned order dates with zeros length: " + str(len(order_comp_with_zeros_cleaned)))

##################################################
# assigning category to materials
final_order_comp_dt = pd.merge(left=order_comp_with_zeros_cleaned, right=cv_result, how="left",
                                  left_on=["customernumber", 'mat_no'], right_on=["customernumber", 'mat_no'])

################################################
# order date basis comparison
print("\n#####################--Frequency wise Prediction Results--#################")
for dataset in ['HFP', 'MFP', 'LFP']:
    # file save directory
    path = image_dir + dataset + sub_file_name
    if not os.path.isdir(path):
        os.makedirs(path)
    else:
        shutil.rmtree(path=path)
        os.makedirs(path)

    if dataset == "HFP":
        order_comp_cat = final_order_comp_dt.loc[final_order_comp_dt['cat'].isin(['I', 'II', 'III', 'VII'])]
        # od_order_comp_cat = order_comp_HFP.copy()
        print('\nselected high frequency customer order compare data:')
        # print(order_comp_cat.head())

    elif dataset == "MFP":
        order_comp_cat = final_order_comp_dt.loc[final_order_comp_dt['cat'].isin(['IV', 'V', 'VI', 'VIII', 'IX'])]
        # od_order_comp_cat = order_comp_MFP.copy()
        print('\nselected medium frequency customer order compare data:')
        # print(order_comp_cat.head())
        # print(order_comp_cat.info())
    elif dataset == "LFP":
        order_comp_cat = final_order_comp_dt.loc[final_order_comp_dt['cat'].isin(['X'])]
        # od_order_comp_cat = order_comp_LFP.copy()
        print('\nselected low frequency customer order compare data:')
        # print(order_comp_cat.head())
        # print(order_comp_cat.info())

    ##############################
    # per order quantity
    ##############################
    # if len(order_comp_cat) == 0:
    #     continue
    #
    # per_order_quantity = order_comp_cat.groupby(['customernumber', 'mat_no'])['actual_q'].\
    #     apply(lambda x: sum(x)/len(x)).rename("per_od_q").reset_index()
    #
    # per_order_quantity['cus_mat'] = per_order_quantity['customernumber'].map(str) + "_" + per_order_quantity['mat_no'].map(str)
    # print("\n per order quantity distribution")
    # print(per_order_quantity.describe())
    #
    # # print(len(order_comp_cat))
    # order_comp_cat['cus_mat'] = order_comp_cat['customernumber'].map(str) + "_" + order_comp_cat[
    #     'mat_no'].map(str)
    # # order_comp_cat = order_comp_cat.groupby(['customernumber', 'mat_no']).filter(lambda x: (x['customernumber'].map(str) + "_" + x['mat_no'].map(str)).isin(list(per_order_quantity.loc[per_order_quantity['per_od_q']>3,'cus_mat'].unique())))
    # order_comp_cat = order_comp_cat.loc[order_comp_cat['cus_mat'].
    #     isin(list(per_order_quantity.loc[per_order_quantity['per_od_q']>1,'cus_mat'].unique()))].reset_index(drop=True)
    # order_comp_cat.drop("cus_mat", axis = 1, inplace = True)
    # # del order_comp_cat['cus_mat']
    # # print((order_comp_cat))

    if len(order_comp_cat) == 0:
        continue

    #################################################
    # OD basis result analysis
    #################################################
    # error metric calculation
    order_comp_cat[['order_date']] = order_comp_cat[['order_date']].apply(lambda x: x.astype(str).apply(parser.parse))
    od_order_comp = order_comp_cat.copy()

    od_order_comp['q_diff_abs'] = abs(od_order_comp['actual_q'] - od_order_comp['pred_q'])
    od_order_comp['perc_diff_abs'] = abs(od_order_comp['actual_q'] - od_order_comp['pred_q']) / \
                                     od_order_comp['actual_q'] * 100
    od_order_comp['q_diff'] = od_order_comp['pred_q'] - od_order_comp['actual_q']
    # od_order_comp['perc_diff'] = 0
    # order_comp_cat= order_comp_cat.reset_index(drop=True)
    # print('\n ########################')
    # print(od_order_comp)
    od_order_comp['perc_diff'] = od_order_comp.apply(lambda x: (x['q_diff'] * 100) if x['actual_q'] == 0 else ((x['pred_q'] - x['actual_q']) / x['actual_q'] * 100), axis=1)

    od_order_comp['perc_diff_bucket'] = od_order_comp['perc_diff'].apply(lambda x: perc_diff_bucket(x))

    od_order_comp.drop(['dd_actual', 'dd_pred', 'month'], axis=1, inplace=True)
    # print('final order date compare data:\n')
    # print(od_order_comp.head())
    # print(od_order_comp.info())

    # persistence
    ###########################################
    if logic_new == True:
        od_order_comp['pred_dec'] = pd.to_numeric(od_order_comp['pred_dec'], errors='coerce')
        od_order_comp= od_order_comp.loc[~od_order_comp['pred_dec'].isnull()]
        persist_dt_cleaned = od_order_comp.copy().sort_values(['customernumber', 'mat_no', 'order_date'])
        # persist_dt['pred_dec'] = pd.to_numeric(persist_dt['pred_dec'], errors='coerce')
        # persist_dt_cleaned = persist_dt.loc[~persist_dt['pred_dec'].isnull()]

        persist_desc = persist_count(persist_dt_cleaned)
        # persist_desc = pd.DataFrame(persist_desc.astype(np.int))
        persist_desc['cus_mat'] = persist_desc['customernumber'].map(str) + "_" + persist_desc['mat_no'].map(str)
        persist_desc.to_csv(path + "persist_count.csv", index=False)
        print("\n persistence count data:")
        print(persist_desc.head())
        print(persist_desc.dtypes)
        print(persist_desc.describe())

        persistence_count = (sum(persist_desc['persist_length']) - len(persist_desc))

        dec_reset_count = len(
            od_order_comp.loc[(od_order_comp['actual_q'] >= 1) & (od_order_comp['pred_dec'] < 1)])

        perfect_match_count = len(od_order_comp.loc[(od_order_comp['actual_q'] == od_order_comp['pred_q'])])

        hit_mismatch_count = len(od_order_comp.loc[(od_order_comp['actual_q'] >= 1) & (od_order_comp['pred_q'] >= 1) & (
                    od_order_comp['actual_q'] != od_order_comp['pred_q'])])

        persist_perc = (sum(persist_desc['persist_length']) - len(persist_desc))/len(od_order_comp)

        dec_reset_perc = len(od_order_comp.loc[(od_order_comp['actual_q'] >=1) & (od_order_comp['pred_dec'] < 1)])/len(od_order_comp)

        perfect_match_perc = len(od_order_comp.loc[(od_order_comp['actual_q'] == od_order_comp['pred_q'])])/len(od_order_comp)

        hit_mismatch_perc = len(od_order_comp.loc[(od_order_comp['actual_q'] >= 1) & (od_order_comp['pred_q'] >= 1) & (od_order_comp['actual_q'] != od_order_comp['pred_q'])])/len(od_order_comp)

        predict_type_desc = pd.DataFrame({'persist_perc' : [persist_perc], 'dec_reset_perc' : [dec_reset_perc],
                                          'perfect_match_perc' : [perfect_match_perc], 'hit_mismatch_perc' : [hit_mismatch_perc],
                                          'persist_count': [persistence_count], 'dec_reset_count': [dec_reset_count],
                                          'perfect_match_count': [perfect_match_count],
                                          'hit_mismatch_count': [hit_mismatch_count]})
        print(predict_type_desc)

        predict_type_desc.to_csv(path + "predict_type_desc.csv", index=False)

    # week difference
    ###########################################
    week_diff_dt = od_order_comp.copy().sort_values(['customernumber', 'mat_no', 'order_date'])
    od_comp_week_diff_act_order = week_diff_dt.loc[week_diff_dt['actual_q'] > 0].\
        reset_index(drop = True).groupby(['customernumber', 'mat_no', 'order_date']).apply(lambda x:np.min(np.absolute(
        (x['order_date'].values[0] - week_diff_dt.loc[(week_diff_dt['customernumber'] == x['customernumber'].values[0]) &
                                           (week_diff_dt['mat_no'] == x['mat_no'].values[0]) &
                                           (week_diff_dt['order_date'] != x['order_date'].values[0]) &
                                           (week_diff_dt['pred_q'] > 0), 'order_date']))).days).rename("day_diff").\
        reset_index()
    od_comp_week_diff_act_order = od_comp_week_diff_act_order.loc[od_comp_week_diff_act_order['day_diff'].notnull()]

    ################################################
    # Error plots
    ################################################
    path = image_dir + dataset + sub_file_name + "OD\\"
    if not os.path.isdir(path):
        os.makedirs(path)

    plot_count_hist(data=od_order_comp, field='q_diff_abs',
                    title='Histogram of Abs Error Quantity on Order Date Basis with zeros',
                    x_label='Error Quantity(cs)',
                    num_bar=10, x_lim=10.5, image_dir=path)

    plot_count_hist(data=od_order_comp.loc[(od_order_comp['q_diff'] >= -5) &
                                                              (od_order_comp['q_diff'] <= 5)],
                    field='q_diff',
                    title='Histogram of Error Quantity on Order Date Basis with zeros(pred-actual)',
                    x_label='Error Quantity(cs)',
                    num_bar=11, x_lim=10.5, image_dir=path)

    plot_count_hist(data=od_order_comp, field='perc_diff_bucket',
                    title='Histogram of % Error Quantity on Order Date Basis with zeros(pred-actual)', x_label='% Error',
                    num_bar=9, x_lim=9.5, image_dir=path)

    for i in range(-4, 5):

        if i in od_order_comp.loc[(od_order_comp['q_diff'] == i)]['q_diff'].unique():
            bar_count = (len(list(od_order_comp.loc[(od_order_comp['q_diff'] == i)][
                                      'actual_q'].unique())))
            plot_count_hist(data=od_order_comp.loc[(od_order_comp['q_diff'] == i)],
                            field='actual_q',
                            title='Histogram actual quantity for error=' + str(i) + " (pred-actual)",
                            x_label='Actual Order(cs)',
                            num_bar=bar_count, x_lim=bar_count + 0.5, image_dir=path)

    if dataset == "LFP": bar_count = 30
    else: bar_count = 20

    plot_count_hist(data=od_comp_week_diff_act_order, field='day_diff',
                    title='Histogram of days Difference for all the actual orders', x_label='day difference',
                    num_bar=bar_count, x_lim=bar_count + 0.5, image_dir=path)

    print("\n####################################################")

    # ###########################################################
    # # Weekly basis analysis
    # ###########################################################
    # # weekly aggregation
    # w_agg_order_comp = order_comp_cat.groupby(['customernumber', 'mat_no',
    #                                           pd.Grouper(key='order_date', freq='W-MON', closed = 'left',
    #                                                      label = 'left')])[['actual_q', 'pred_q']].sum() \
    #     .reset_index().sort_values(['customernumber', 'mat_no', 'order_date'])
    #
    # print("\nWeekly agg data:")
    # print(w_agg_order_comp.head())
    #
    # w_agg_order_comp['q_diff_abs'] = abs(w_agg_order_comp['actual_q'] - w_agg_order_comp['pred_q'])
    # w_agg_order_comp['perc_diff_abs'] = abs(w_agg_order_comp['actual_q'] - w_agg_order_comp['pred_q']) / \
    #                                  w_agg_order_comp['actual_q'] * 100
    # w_agg_order_comp['q_diff'] = w_agg_order_comp['pred_q'] - w_agg_order_comp['actual_q']
    # w_agg_order_comp['perc_diff'] = w_agg_order_comp.apply(lambda x: (x['q_diff'] * 100) if x['actual_q'] == 0 else
    # ((x['pred_q'] - x['actual_q']) / x['actual_q'] * 100), axis=1)
    #
    # w_agg_order_comp['perc_diff_bucket'] = w_agg_order_comp['perc_diff'].apply(lambda x: perc_diff_bucket(x))
    #
    # # Error plots
    # ################################################
    # path = image_dir + dataset + sub_file_name + "Weekly\\"
    # if not os.path.isdir(path):
    #     os.makedirs(path)
    #
    # plot_count_hist(data=w_agg_order_comp, field='q_diff_abs',
    #                 title='Histogram of Abs Error Quantity on Weekly Basis with zeros',
    #                 x_label='Error Quantity(cs)',
    #                 num_bar=10, x_lim=10.5, image_dir=path)
    #
    # plot_count_hist(data=w_agg_order_comp.loc[(w_agg_order_comp['q_diff'] >= -5) &
    #                                                           (w_agg_order_comp['q_diff'] <= 5)],
    #                 field='q_diff',
    #                 title='Histogram of Error Quantity on Weekly Basis with zeros(pred-actual)',
    #                 x_label='Error Quantity(cs)',
    #                 num_bar=11, x_lim=10.5, image_dir=path)
    #
    # plot_count_hist(data=w_agg_order_comp, field='perc_diff_bucket',
    #                 title='Histogram of % Error Quantity on Weekly Basis with zeros(pred-actual)', x_label='% Error',
    #                 num_bar=9, x_lim=9.5, image_dir=path)
    #
    # for i in range(-4, 5):
    #
    #     if i in w_agg_order_comp.loc[(w_agg_order_comp['q_diff'] == i)]['q_diff'].unique():
    #
    #         bar_count = (len(list(w_agg_order_comp.loc[(w_agg_order_comp['q_diff'] == i)][
    #                                   'actual_q'].unique())))
    #         plot_count_hist(data=w_agg_order_comp.loc[(w_agg_order_comp['q_diff'] == i)],
    #                         field='actual_q',
    #                         title='Histogram actual quantity for error=' + str(i) + " (pred-actual)",
    #                         x_label='Actual Order(cs)',
    #                         num_bar=bar_count, x_lim=bar_count + 0.5, image_dir=path)
    #     else:
    #         print("\nError difference not available in the list.")


################################################
# customer aggregate
################################################
od_customer_agg = \
final_order_comp_dt[['customernumber', 'mat_no', 'order_date', 'actual_q', 'pred_q']].groupby(
    ['customernumber', 'order_date'])['actual_q', 'pred_q'].sum().reset_index()

od_customer_agg['diff'] = od_customer_agg['pred_q'] - od_customer_agg['actual_q']
od_customer_agg['perc_diff'] = (od_customer_agg['pred_q'] - od_customer_agg['actual_q'])/od_customer_agg['actual_q']*100
od_customer_agg['abs_perc_diff'] = abs(od_customer_agg['perc_diff'])
#####################################################
