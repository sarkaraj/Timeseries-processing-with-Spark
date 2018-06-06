# from model.weekly_model import *
# from model.monthly_model import *
# from transform_data.data_transform import *
# from model.moving_average import *
# from model.monthly_pydlm import *
# from model.plt_data import *
# from model.moving_average_monthly import *
# from distributed_grid_search._fbprophet_monthly import *
# from distributed_grid_search._fbprophet import *
from distributed_grid_search._model_params_set import *
from distributed_grid_search.properties import *
from distributed_grid_search._sarimax_monthly import *
from distributed_grid_search._sarimax import *
from model.save_images import *

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
image_dir = "C:\\files\\CONA_Conv_Store_Data\\temp\\thadeus\\monthly_trend_analysis\\"

# holidays
# holidays = pd.read_table(file_dir + 'holidays.csv', delimiter=',', header=0)
# holidays.ds = holidays.ds.apply(parser.parse)
# holidays.lower_window = -7
# holidays.upper_window = 7
# holidays.head(6)

def generate_param():
    """
    CONDITIONS::::
    yearly_seasonality = True / False
    seasonality_prior_scale
    {0.1, 0.2, _______, 1)

    ###########################

    changepoint_prior_scale
    {1, 2________, 10}

    :return:
    """
    import numpy as np

    yearly_seasonality = [True, False]
    seasonality_prior_scale = [{'seasonality_prior_scale': round(i / 10.0, 2)} for i in
                               np.arange(PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_M_SEASONALITY_PRIOR_SCALE_STEP_SIZE)]

    # TODO: Added to test model improvement, can be removed later
    optional_seeasonality_prior_scale = [{'seasonality_prior_scale': 0.05}]
    seasonality_prior_scale = seasonality_prior_scale + optional_seeasonality_prior_scale

    changepoint_prior_scale = [{'changepoint_prior_scale': int(i)} for i in
                               np.arange(PROPH_M_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_M_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_M_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE)]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in list(itertools.product(changepoint_prior_scale, yearly_seasonality_all_combo))]

    return _result

# data transformation to weekly and monthly aggregate
raw_data = pd.read_csv(file_dir + "ThaddeusSmithConvRawInvoice.tsv",
                       sep="\t", header=0, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

print(max(raw_data.date))
print(min(raw_data.date))
cus_no = 500068482
mat_no = 134929

raw_data.quantity = raw_data.quantity.apply(float)
raw_data = raw_data.loc[raw_data['quantity'] >= 0]

cus_test = raw_data[raw_data.customernumber == cus_no]
prod_test = cus_test[cus_test.matnr == mat_no]

# data_weekly = get_weekly_aggregate(inputDF=prod_test)
# data_weekly.dt_week = data_weekly.dt_week.apply(str).apply(parser.parse)

# weekly_data = data_weekly.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

# monthly_data = get_monthly_aggregate_per_product(weekly_data)

# data_weekly.head()

# data_monthly = get_monthly_aggregate(inputDF=raw_data)
# data_monthly.dt_week = data_monthly.dt_week.apply(str).apply(parser.parse)
# print (data_monthly.head())

# param = {'changepoint_prior_scale': 2, 'yearly_seasonality': True, 'seasonality_prior_scale': 0.2}
# loop to run for all product, all customer
# final_data_df = pd.DataFrame()
# for cus_no in raw_data.customernumber.unique():
#     cus = raw_data[raw_data.customernumber == cus_no]
#     for mat_no in cus.matnr.unique():
#         prod = cus[cus.matnr == mat_no]
#         prod['quantity'] = prod['quantity'].apply(float)
#         prod = prod.loc[prod['quantity'] >= 0]
#         # prod = prod.loc[prod['quantity'] >= 0]
#
#         print(prod.head())
        # prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
        # prod.ds = prod.ds.apply(str).apply(parser.parse)
        # prod.y = prod.y.apply(float)
        # prod = prod.sort_values('ds')
        # prod = prod.reset_index(drop=True)

        # plot_weekly_data(data=prod, dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)

monthly_data = get_monthly_aggregate(prod_test)
print(monthly_data)

        # plot_monthly_data(data=monthly_data, dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)


        # days = (
        # max(prod.dt_week.apply(str).apply(parser.parse)) - min(prod.dt_week.apply(str).apply(parser.parse))).days
        # if days > 854:
        # weekly_model_image_saver(prod, cus_no, mat_no, dir_name, holidays, min_train_days=731, test_points=2)
        # prod_output = weekly_ensm_model(prod=prod, cus_no=cus_no, mat_no=mat_no, dir_name=image_dir)

        # monthly_prophet_model(prod, cus_no, mat_no, dir_name, min_train_days=731, test_points=1)
        # prod_output = monthly_prophet_model(prod = prod , cus_no = cus_no, mat_no = mat_no, dir_name= image_dir)
        # prod_output = monthly_pydlm_model(prod = prod , cus_no = cus_no, mat_no = mat_no)
        # (prod_output, pred) = moving_average_model(prod=prod, cus_no=cus_no, mat_no=mat_no, weekly_data=False,
        #                                             weekly_window=6, monthly_window=3, pred_points= 2)

        # prod_output = moving_average_model_monthly(prod = prod, cus_no= cus_no, mat_no=mat_no)
        # res = run_prophet_monthly(cus_no= cus_no, mat_no= mat_no, prod=prod, param= param, min_train_days= 731)
        # prod_output = res[1][1]
#
# for elem in generate_all_param_combo_sarimax_monthly():
#     print(elem)
#     output = sarimax_monthly(cus_no=cus_no, mat_no=mat_no, prod=monthly_data, pdq=elem[0],
#                                                        seasonal_pdq= elem[1], trend=elem[2])

output = sarimax_monthly(cus_no=cus_no, mat_no=mat_no, prod=monthly_data, pdq=(2,0,1), seasonal_pdq= (1,0,0,12), trend=[0,0,1]
                         ,enforce_stationarity = False, enforce_invertibility = False, measurement_error = True, hamilton_representation = False)
# print("elem")
# print(elem)
# print("output")
print(output)

# if (output not in ["MODEL_NOT_VALID"]):
#     three_dim_save_plot(x1= output[3].ds, y1= output[3].y, y1_label= "actual",
#                       x2= output[1].ds, y2= output[1].y_ARIMA, y2_label='predicted',
#                       x3=output[2].ds, y3=output[2].y_ARIMA, y3_label='model_fit',
#                       xlable= "Date", ylable= "Quantity", title= "ts", dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)



            # final_data_df = pd.concat([final_data_df, output[1][1]], axis=0)

# final_data_df.to_csv(image_dir + "error.csv", sep=',', header=True)
#
# print (final_data_df.head())

# for cus_no in raw_data.customernumber.unique():
# for cus_no in raw_data.customernumber.unique():
#     cus = raw_data[raw_data.customernumber == cus_no]
#     for mat_no in cus.matnr.unique():
#         prod = cus[cus.matnr == mat_no]
#         prod.date = prod.date.apply(str).apply(parser.parse)
#         prod.y = prod.quantity.apply(float)
#         prod = prod.sort_values('date')
#         prod = prod.reset_index(drop=True)
#         plot_raw_data(data=prod, dir_name=image_dir, cus_no=cus_no, mat_no=mat_no)