import numpy as np
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
from transform_data.data_transform import get_weekly_aggregate, get_monthly_aggregate
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
raw_data = pd.read_csv(raw_data_dir + "raw_invoices_2018-06-19.tsv",
                       sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

# read cv result data to figure out high frequency products
cv_result = pd.read_csv(cv_result_dir + "cat_123_2018-06-19.tsv",
                          sep= "\t", header=0)

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

for i in range(len(cv_result)):

    cus_no = cv_result['customernumber'][i]
    mat_no = cv_result['mat_no'][i]

    #####################-- obtaining per combination aggregated invoices data --##################
    ## for weekly it has to be sunday, monthly last dte of month
    mdl_cutoff_date = parser.parse("2018-06-17")

    # filtering data
    cus = raw_data[raw_data.customernumber == cus_no]
    prod = cus[cus.matnr == mat_no]

    prod.date = prod.date.apply(str).apply(parser.parse)
    prod.quantity = prod.quantity.apply(float)
    prod = prod.sort_values('date')
    prod = prod.reset_index(drop=True)

    prod = prod.loc[prod['quantity'] >= 0.0]
    prod = prod.loc[prod['date'] <= mdl_cutoff_date]

    # artificially adding 0.0 at mdl cutoff date to get the aggregate right
    lst_point = pd.DataFrame({'customernumber': [cus_no], 'matnr': [mat_no], 'date': [mdl_cutoff_date],
                              'quantity': [0.0], 'q_indep_p': [0.0]})

    prod = prod.append(lst_point, ignore_index=True)
    prod = prod.reset_index(drop=True)

    data_w_agg = get_weekly_aggregate(inputDF=prod)
    data_w_agg = data_w_agg.sort_values('dt_week')
    print("Weekly aggregated data:\n")
    print(data_w_agg)
    print("#####################################################\n")

    #####################-- obtaining promotions data per combination --##################







