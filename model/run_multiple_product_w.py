from transform_data.data_transform import *
from model.ma_outlier import *
from distributed_grid_search._sarimax import *
from distributed_grid_search._sarimax_monthly import *
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
import time

rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

cv_result_dir = "C:\\CONA_CSO\\thadeus_route\\cv_result\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\weekly_wre_12\\"

raw_data = pd.read_csv(file_dir + "raw_invoices.tsv",
                       sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

m_cv_result = pd.read_csv(cv_result_dir + "cat_123_2018-06-12.tsv",
                          sep= "\t", header=0)

print("Raw Data Head:\n")
print(raw_data.head())

print("Monthly CV Result:\n")
print(m_cv_result.dtypes)

# print(m_cv_result.arima_params)

m_cv_result['arima_params_dict'] = m_cv_result['arima_params'].map(lambda x: [x.replace("]", "[").split("[")[i] for i in[1,3]])
m_cv_result['arima_params_dict'] = m_cv_result['arima_params_dict'].map(lambda x: {'seasonal_pdq' : tuple(map(int,x[0].split(","))),
                                                                                   'pdq': tuple(map(int,x[1].split(",")))})

# print(m_cv_result.head())
for i in range(len(m_cv_result)):
    # User Input
    ###########################################################
    cus_no = m_cv_result['customernumber'][i]
    mat_no = m_cv_result['mat_no'][i]

    ## for weekly it has to be sunday, monthly last dte of month
    mdl_cutoff_date = parser.parse("2018-06-03") #"2018-06-03"

    pdq = m_cv_result['arima_params_dict'][i].get('pdq')
    pdq_seasonal = m_cv_result['arima_params_dict'][i].get('seasonal_pdq')
    ############################################################

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

    # if weekly_model == True:
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

    data_w_agg_cleaned = ma_replace_outlier(data=data_w_agg, n_pass=3, aggressive=True, sigma=3) # initially sigma was 2.5

    two_dim_save_plot(x1= raw_w_agg_data.ds, y1= raw_w_agg_data.y, y1_label= "Raw_data",
                      x2= data_w_agg_cleaned.ds, y2= data_w_agg_cleaned.y, y2_label= "Cleaned_data",
                      xlable= "Date", ylable= "Quantity",
                      title= "Raw_vs_Cleaned_Data", cus_no= cus_no, mat_no= mat_no, dir_name= image_dir)

    #sarimax(cus_no, mat_no, pdq, seasonal_pdq, prod, run_locally=False, **kwargs):
    output = sarimax(cus_no= cus_no, mat_no= mat_no, pdq= pdq, seasonal_pdq= pdq_seasonal, prod= data_w_agg_cleaned,
                     run_locally= True, image_dir= image_dir)

    print("Output sarimax model:")
    print(output)
    print("\n#####################################################")
    print("--- %s seconds ---" % (time.time() - start_time))
