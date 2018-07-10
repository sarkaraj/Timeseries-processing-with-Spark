import numpy as np
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
from transform_data.data_transform import get_weekly_aggregate, get_monthly_aggregate
from model.save_images import *
from transform_data.holidays import *
# import time
rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\CONA_CSO\\thadeus_route\\promotion_detail\\Promotion_Details_Final\\"
raw_data_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

# cv results stores, used to find out high frequency products
cv_result_dir = "C:\\CONA_CSO\\thadeus_route\\cv_result\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\promotions_plots\\"

#####################################################################################################
# data load and data preparation(one time job)
#####################################################################################################
# promotions_2018 = pd.read_csv(file_dir + "2018 Promotions.csv", sep = ",", header= 0, index_col= 0)
# # promotions_2018.to_csv(file_dir + "2018 Promotions.csv")
# print("Promotions 2018:\n")
# print(promotions_2018.head())
# print("#######################################################\n")
# promotions_2014_to_2017 = pd.read_csv(file_dir + "2014 to 2017 Promotions.csv", sep = ",", header= 0, index_col= 0)
# # promotions_2014_to_2017.to_csv(file_dir + "2014 to 2017 Promotions.csv")
# print("Promotions 2014 to 2017:\n")
# print(promotions_2014_to_2017.head())
# print("#######################################################\n")
# promotion_details = pd.read_csv(file_dir + "Promotion Details Final.csv", sep = ",", header= 0, index_col= 0)
# # promotion_details.to_csv(file_dir + "Promotion Details Final.csv")
# print("Promotions details:\n")
# print(promotion_details.head())
# # print(promotion_details.groupby(['Number'])['Number'].count())
# print("#######################################################\n")
# product_segment_and_products = pd.read_csv(file_dir + "Product Segment and Products.csv",sep = ",", header= 0,
#                                            index_col= 0, encoding = "ISO-8859-1")
# # product_segment_and_products.to_csv(file_dir + "Product Segment and Products.csv")
# print("Product segment and products:\n")
# print(product_segment_and_products.head())
# print("#######################################################\n")
#
# # merging all promotions
# promotions = promotions_2018.append(promotions_2014_to_2017, ignore_index= True)
# print("Promotions Combined:\n")
# print(len(promotions))
# # print(promotions.groupby(['Number'])['Number'].count())
# print("#######################################################\n")
#
# # merging promotions and promotions detains
# p_and_p_details = pd.merge(left= promotions, right= promotion_details,how= 'left',
#                            left_on = 'Number', right_on= 'Number')
# print("Promotions and Promotions Details:\n")
# # p_and_p_details['Product Segment ID'] = p_and_p_details['Product Segment ID'].astype(float)
# print(len(p_and_p_details))
# print(p_and_p_details.dtypes)
# print(p_and_p_details.isnull().sum())
# p_and_p_details.to_csv(file_dir + "promotions_and_promotions_details.csv")
# p_and_p_details_missing_details = p_and_p_details[p_and_p_details['Product Segment ID'].isnull()]
# p_and_p_details_missing_details.to_csv(file_dir + "promotions_details_missing.csv")
# print("#######################################################\n")
#
# # cross join on product segment data frame
# p_and_p_details = p_and_p_details[p_and_p_details['Product Segment ID'].notnull()]
# product_segment_and_products = product_segment_and_products[product_segment_and_products['Target Group ID'].notnull()]
# product_segment_and_products['Target Group ID'] = product_segment_and_products['Target Group ID'].astype(float)
#
# final_all_promotions = pd.merge(left= p_and_p_details, right= product_segment_and_products, how = "left",
#                                 left_on="Product Segment ID", right_on= "Target Group ID")
#
# print("Promotions and Promotions Details pre cleanup:\n")
# print(final_all_promotions.head())
# print(len(final_all_promotions))
# print(final_all_promotions.dtypes)
# print(final_all_promotions.isnull().sum())
#
# final_all_promotions = final_all_promotions[final_all_promotions['Target Group ID_y'].notnull()]
# final_all_promotions = final_all_promotions[final_all_promotions['Spend Method'].notnull()]
# final_all_promotions = final_all_promotions.drop(['Plnd Start_y', 'Plan. Fin._y', 'Target Group ID_y'], axis=1)
# final_all_promotions = final_all_promotions.rename(columns={'Target Group ID_x': 'Target Group ID',
#                                                             'Plnd Start_x': 'Plnd Start', 'Plan. Fin._x': 'Plan Fin'})
#
# print("Promotions and Promotions Details post cleanup:\n")
# print(final_all_promotions.head())
# print(len(final_all_promotions))
# print(final_all_promotions.dtypes)
# print(final_all_promotions.isnull().sum())
# final_all_promotions.to_csv(file_dir + "final_all_promotions.csv")
# print("#######################################################\n")
#####################################################################################################

#####################################################################################################
# Data Issues:
# 11% promotions does not have promotions details
# 10 promotions ID has 2 entries in the promotions details data
# for one of the promotion spend method is missing
#####################################################################################################


# read complete promotions data(one time job)
# promotions_data = pd.read_csv(file_dir + "final_all_promotions.csv", sep = ",", header= 0,
#                               index_col=0, encoding = "ISO-8859-1")
# print("Promotions Data:\n")
# print(promotions_data.head())
# print(len(promotions_data))
# print(promotions_data.dtypes)
# print(promotions_data.isnull().sum())
# print('###########################################################\n')

# read raw invoices data
raw_data = pd.read_csv(raw_data_dir + "raw_invoices_till_2018-06-24.tsv",
                       sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

# read cv result data to figure out high frequency products
cv_result = pd.read_csv(cv_result_dir + "cat_123_2018-06-25.tsv",
                          sep= "\t", header=None, names= ["customernumber", "mat_no", "rmse", "cum_error_ar",
                                                          "period_days_ar", "wre_max_12_ar", "wre_med_12_ar",
                                                          "wre_mean_12_ar", "wre_max_6_ar", "wre_med_6_ar",
                                                          "wre_mean_6_ar", "arima_params", "quantity_mean_6",
                                                          "quantity_mean_12", "cum_quantity", "cat", "mdl_bld_dt"])

print("CV_result:\n")
print(cv_result.head())
print(cv_result.dtypes)
print('###########################################################\n')

# filter the promotions data for the route(one time job)
# route_promotions_data = promotions_data[promotions_data['Business Partner'].isin(np.array(raw_data['customernumber']))]
# route_promotions_data.to_csv(file_dir + "thadeus_route_promotions.csv")

route_promotions_data = pd.read_csv(file_dir + "thadeus_route_promotions.csv", sep = ",", header= 0,
                                    index_col=0, encoding = "ISO-8859-1")
print("route promotions data:\n")
print(route_promotions_data.head())
print(route_promotions_data.dtypes)
print('###########################################################\n')

# filter spnd type ZEDV
route_promotions_data_non_ZEDV = route_promotions_data[route_promotions_data['Spnd Type'] != 'ZEDV']
# route_promotions_data_ZEDV = route_promotions_data[route_promotions_data['Spnd Type'] == 'ZEDV']
print("non ZEDV Promotions Data:\n")
print(route_promotions_data_non_ZEDV.head())
print(len(route_promotions_data_non_ZEDV))
print('###########################################################\n')

# obtaining holidays data
##################################################################
_holidays = get_holidays_dataframe_pd()
_holidays['week_before'] = _holidays['ds'] + pd.DateOffset(days = -7)
_holidays['week_after'] = _holidays['ds'] + pd.DateOffset(days = 7)
print(_holidays.head())
print('###########################################################\n')

# temp: zedv data eda
################################################################
# print(route_promotions_data_ZEDV.dtypes)
# route_promotions_data_ZEDV.loc['Plnd_Start_date'] = route_promotions_data_ZEDV['Plnd Start'].apply(str).apply(parser.parse)
# route_promotions_data_ZEDV.loc['Plan_Fin_date'] = route_promotions_data_ZEDV['Plan Fin'].apply(str).apply(parser.parse)
#
# route_promotions_data_ZEDV.loc['promo_period'] = route_promotions_data_ZEDV.apply(lambda x: (x['Plan_Fin_date'] - x['Plnd_Start_date']).days)
#
# print(route_promotions_data_ZEDV)
################################################################


# print(route_promotions_data_non_ZEDV.groupby(['Spnd Type'])['Spnd Type'].count())


############################################################################################
# plot holidays and promotion er customer product
############################################################################################
# for i in range(len(cv_result)):
#
#     cus_no = cv_result['customernumber'][i]
#     mat_no = cv_result['mat_no'][i]
#
#     #####################-- obtaining per combination aggregated invoices data --#####################
#     ## for weekly it has to be sunday, monthly last date of month
#     mdl_cutoff_date = parser.parse("2018-06-17")
#
#     # filtering data
#     cus = raw_data[raw_data.customernumber == cus_no]
#     prod = cus[cus.matnr == mat_no]
#
#     prod.date = prod.date.apply(str).apply(parser.parse)
#     prod.quantity = prod.quantity.apply(float)
#     prod = prod.sort_values('date')
#     prod = prod.reset_index(drop=True)
#
#     prod = prod.loc[prod['quantity'] >= 0.0]
#     prod = prod.loc[prod['date'] <= mdl_cutoff_date]
#
#     # artificially adding 0.0 at mdl cutoff date to get the aggregate right
#     lst_point = pd.DataFrame({'customernumber': [cus_no], 'matnr': [mat_no], 'date': [mdl_cutoff_date],
#                               'quantity': [0.0], 'q_indep_p': [0.0]})
#
#     prod = prod.append(lst_point, ignore_index=True)
#     prod = prod.reset_index(drop=True)
#
#     data_w_agg = get_weekly_aggregate(inputDF=prod)
#     data_w_agg = data_w_agg.sort_values('dt_week')
#     print("Weekly aggregated data:\n")
#     print(data_w_agg)
#     print("#####################################################\n")
#
#     data_w_agg = data_w_agg[['dt_week', 'quantity']]
#     data_w_agg = data_w_agg.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
#
#     data_w_agg.ds = data_w_agg.ds.apply(str).apply(parser.parse)
#     data_w_agg.y = data_w_agg.y.apply(float)
#     data_w_agg = data_w_agg.sort_values('ds')
#     data_w_agg = data_w_agg.reset_index(drop=True)
#
#     #####################-- obtaining promotions data per combination --########################
#
#     cus_promo = route_promotions_data_non_ZEDV[route_promotions_data['Business Partner'] == cus_no]
#     prod_promo = cus_promo[cus_promo['Product ID'] == mat_no]
#
#     prod_promo['Plnd Start'] = prod_promo['Plnd Start'].apply(parser.parse)
#     prod_promo['Plan Fin'] = prod_promo['Plan Fin'].apply(parser.parse)
#
#     prod_promo = prod_promo[prod_promo['Plan Fin'] >= min(data_w_agg['ds'])]
#
#     print("Cus-Mat Promotions Data:\n")
#     print(prod_promo[['Business Partner', 'Product ID', 'Plnd Start', 'Plan Fin', 'Spend Method', 'Spnd Type']].head())
#     print("#####################################################\n")
#
#     prod_holidays = _holidays[_holidays['week_after'] >= min(data_w_agg['ds'])]
#     prod_holidays = prod_holidays[prod_holidays['week_after'] <= max(data_w_agg['ds'] + pd.DateOffset(14))]
#
#     print("Holidays List:\n")
#     print(prod_holidays.tail())
#     print("#####################################################\n")
#
#     promotions_and_holidays_save_plots(x= data_w_agg.ds, y = data_w_agg.y,
#                                        xlable= "Date", ylable= "Quantity",
#                                        promo_count= len(prod_promo), start_day_list= np.array(prod_promo['Plnd Start']),
#                                        end_day_list= np.array(prod_promo['Plan Fin']), spnd_type= np.array(prod_promo['Spnd Type']),
#                                        holidays_count= len(prod_holidays), holiday_start_day_list= prod_holidays['week_before'],
#                                        holidays_end_day_list= prod_holidays['week_after'], holiday_name= np.array(prod_holidays.holiday),
#                                        title= "Holiday_Effect", dir_name= image_dir, cus_no= cus_no, mat_no= mat_no,
#                                        plot_type= "holidays")
    ############################################################################################














