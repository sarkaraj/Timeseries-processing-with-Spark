from transform_data.data_transform import *
from model.ma_outlier import *
from distributed_grid_search._sarimax import *
from distributed_grid_search._sarimax_monthly import *
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
import time

rcParams['figure.figsize'] = 15, 6

####################################
# user input
mdl_cutoff_date = parser.parse("2018-06-03")
####################################

# data load and transform
file_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

cv_result_dir = "C:\\CONA_CSO\\thadeus_route\\cv_result\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\monthly_rmse\\"

raw_data = pd.read_csv(file_dir + "raw_invoices_2018-06-28.tsv",
                       sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

cv_result = pd.read_csv(cv_result_dir + "cv_result_2018-08-03.tsv",
                          sep= "\t", header= None, names= ["customernumber", "mat_no", "rmse", "mae", "mape", "cum_error", "period_days", "wre_max_6",
                                                           "wre_med_6", "wre_mean_6", "quantity_mean_6", "wre_max_12", "wre_med_12",
                                                           "wre_mean_12", "quantity_mean_12", "wre_max_24", "wre_med_24",
                                                           "wre_mean_24", "quantity_mean_24", "wre_max_48", "wre_med_48",
                                                           "wre_mean_48", "quantity_mean_48", "params", "cat", "mdl_bld_dt"])

print("Raw Data:\n")
print(raw_data.head())
print("CV Result:\n")
print(cv_result.head())

cv_cat_123 = cv_result.loc[cv_result['cat'].isin(["I", "II", "III"])]

print("CV Result cat 123:\n")
print(cv_cat_123.head())

for i in range(len(cv_cat_123)):

    cus_no = cv_cat_123['customernumber'].values[i]
    mat_no = cv_cat_123['mat_no'].values[i]

    print("customer number:\n")
    print(cus_no)
    print("material number:\n")
    print(mat_no)

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
    lst_point = pd.DataFrame({'customernumber': [cus_no],'matnr': [mat_no],'date': [mdl_cutoff_date],
                              'quantity': [0.0], 'q_indep_p': [0.0]})

    prod = prod.append(lst_point,ignore_index=True)
    prod = prod.reset_index(drop= True)

    start_time = time.time()
    data_w_agg = get_weekly_aggregate(inputDF=prod)
    data_w_agg = data_w_agg.sort_values('dt_week')
    print("Weekly aggregated data:\n")
    print(data_w_agg)
    print("#####################################################\n")

    data_w_agg = data_w_agg[['dt_week', 'quantity']]
    data_w_agg = data_w_agg.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

    data_w_agg.ds = data_w_agg.ds.apply(str).apply(parser.parse)
    data_w_agg.y = data_w_agg.y.apply(float)
    data_w_agg = data_w_agg.sort_values('ds')
    data_w_agg = data_w_agg.reset_index(drop=True)

    raw_w_agg_data = data_w_agg.copy()

    data_w_agg_cleaned = ma_replace_outlier(data=data_w_agg, n_pass=3, aggressive=True, sigma=4)

    pdq = [(0,1,1), (0,1,2), (0,2,2)]
    pdq_seasonal = (0,0,0,52)




