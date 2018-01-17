from transform_data.pandas_support_func import *
import pandas as pd
from matplotlib.pylab import rcParams
from dateutil import parser
from model.save_images import *
from model.plt_data import *
from transform_data.data_transform import *
from model.ma_outlier import *

rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\files\\CONA_Conv_Store_Data\\CCBF\\raw_data\\"

# image save folder
image_dir = "C:\\files\\CONA_Conv_Store_Data\\CCBF\\raw_data\\plots\\temp\\"

raw_data = pd.read_csv(file_dir + "raw_invoice_39Stores_CCBF_COMPLETE.tsv", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

prediction = pd.read_csv(file_dir + "predicted_invoice_with_39Stores_CCBF_JULY_TO_NOV.tsv", sep= "\t", header= None,
                         names= ['customernumber', 'matnr', 'order_date', 'delivery_date', 'cso_quantity', 'decimal',
                                 'pred_quantity', 'pred_description'])

prediction['pred_description'] = prediction['pred_description'].apply(lambda x : convert_string_to_array_of_dict(x))

_all_cus_actual_vs_pred_weekly_aggregate = pd.DataFrame()
_all_cus_actual_vs_pred_monthly_aggregate = pd.DataFrame()

for cus_no in prediction.customernumber.unique():
    cus_raw = raw_data[raw_data.customernumber == cus_no]
    cus_pred = prediction[prediction.customernumber == cus_no]
    for mat_no in cus_pred.matnr.unique():
        try:
            prod_raw = cus_raw[cus_raw.matnr == mat_no]
            prod_pred = cus_pred[cus_pred.matnr == mat_no]

            prod_raw.date = prod_raw.date.apply(str).apply(parser.parse)
            prod_raw.quantity = prod_raw.quantity.apply(float)
            prod_raw = prod_raw.sort_values('date')
            prod_raw = prod_raw.reset_index(drop=True)
            # print(len(prod_raw))
            prod_pred = prod_pred.rename(columns={'delivery_date': 'date', 'quantity': 'y'})
            prod_pred.date = prod_pred.date.apply(str).apply(parser.parse)
            prod_pred.cso_quantity = prod_pred.cso_quantity.apply(float)
            prod_pred = prod_pred.sort_values('date')
            prod_pred = prod_pred.reset_index(drop=True)
            # print(len(prod_pred))

            cat = find_dominant_category(data=prediction, cus_no=cus_no, mat_no=mat_no)

            #ploting raw data vs prediction
            # plot_invoice_data_vs_pred(invoice_data=prod_raw, pred_data=prod_pred, dir_name= image_dir,
            #                           cus_no= cus_no, mat_no= mat_no, title= "Raw Data vs Prediction"+"(category-"+cat+")")


            # monthly and weekly aggregate of invoice data
            prod_weekly_raw = get_weekly_aggregate(prod_raw)

            prod_weekly_raw = prod_weekly_raw.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
            prod_weekly_raw = prod_weekly_raw[['customernumber', 'matnr', 'ds', 'y']]
            prod_weekly_raw.ds = prod_weekly_raw.ds.apply(str).apply(parser.parse)
            prod_weekly_raw.y = prod_weekly_raw.y.apply(float)
            prod_weekly_raw = prod_weekly_raw.sort_values('ds')
            prod_weekly_raw = prod_weekly_raw.reset_index(drop=True)

            prod_monthly_raw = get_monthly_aggregate(prod_raw)

            prod_monthly_raw = prod_monthly_raw.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

            prod_weekly_clean = ma_replace_outlier(data=prod_weekly_raw, n_pass=3, aggressive=True, sigma= 2.5)
            prod_monthly_clean = ma_replace_outlier(data=prod_monthly_raw, n_pass=3, aggressive=True, window_size=6,
                                                    sigma=2.5)

            prod_weekly_raw = prod_weekly_raw[['customernumber', 'matnr', 'ds', 'y']]
            prod_weekly_raw.ds = prod_weekly_raw.ds.apply(str).apply(parser.parse)
            prod_weekly_raw.y = prod_weekly_raw.y.apply(float)
            prod_weekly_raw = prod_weekly_raw.sort_values('ds')
            prod_weekly_raw = prod_weekly_raw.reset_index(drop=True)

            prod_weekly_clean = prod_weekly_clean[['customernumber', 'matnr', 'ds', 'y']]
            prod_weekly_clean.ds = prod_weekly_clean.ds.apply(str).apply(parser.parse)
            prod_weekly_clean.y = prod_weekly_clean.y.apply(float)
            prod_weekly_clean = prod_weekly_clean.sort_values('ds')
            prod_weekly_clean = prod_weekly_clean.reset_index(drop=True)

            prod_monthly_raw = prod_monthly_raw[['customernumber', 'matnr', 'ds', 'y']]
            prod_monthly_raw.ds = prod_monthly_raw.ds.apply(str).apply(parser.parse)
            prod_monthly_raw.y = prod_monthly_raw.y.apply(float)
            prod_monthly_raw = prod_monthly_raw.sort_values('ds')
            prod_monthly_raw = prod_monthly_raw.reset_index(drop=True)

            prod_monthly_clean = prod_monthly_clean[['customernumber', 'matnr', 'ds', 'y']]
            prod_monthly_clean.ds = prod_monthly_clean.ds.apply(str).apply(parser.parse)
            prod_monthly_clean.y = prod_monthly_clean.y.apply(float)
            prod_monthly_clean = prod_monthly_clean.sort_values('ds')
            prod_monthly_clean = prod_monthly_clean.reset_index(drop=True)

            prod_weekly_raw = prod_weekly_raw.rename(columns={'ds': 'date', 'y': 'quantity'})
            prod_monthly_raw = prod_monthly_raw.rename(columns={'ds': 'date', 'y': 'quantity'})
            prod_weekly_clean = prod_weekly_clean.rename(columns={'ds': 'date', 'y': 'quantity'})
            prod_monthly_clean = prod_monthly_clean.rename(columns={'ds': 'date', 'y': 'quantity'})

            # monthly and weekly aggregate of prediction data
            prod_pred = prod_pred.rename(columns={'cso_quantity': 'quantity'})
            prod_pred['q_indep_p'] = 0
            prod_pred = prod_pred[['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p']]
            prod_weekly_pred = get_weekly_aggregate(prod_pred)

            prod_weekly_pred = prod_weekly_pred.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

            prod_weekly_pred = prod_weekly_pred[['customernumber','matnr', 'ds', 'y']]
            prod_weekly_pred.ds = prod_weekly_pred.ds.apply(str).apply(parser.parse)
            prod_weekly_pred.y = prod_weekly_pred.y.apply(float)
            prod_weekly_pred = prod_weekly_pred.sort_values('ds')
            prod_weekly_pred = prod_weekly_pred.reset_index(drop=True)

            prod_monthly_pred = get_monthly_aggregate(prod_pred)

            prod_monthly_pred = prod_monthly_pred.rename(columns={'dt_week': 'ds', 'quantity': 'y'})

            prod_monthly_pred = prod_monthly_pred[['customernumber','matnr','ds', 'y']]
            prod_monthly_pred.ds = prod_monthly_pred.ds.apply(str).apply(parser.parse)
            prod_monthly_pred.y = prod_monthly_pred.y.apply(float)
            prod_monthly_pred = prod_monthly_pred.sort_values('ds')
            prod_monthly_pred = prod_monthly_pred.reset_index(drop=True)

            prod_weekly_pred = prod_weekly_pred.rename(columns={'ds': 'date', 'y': 'cso_quantity'})
            prod_monthly_pred = prod_monthly_pred.rename(columns={'ds': 'date', 'y': 'cso_quantity'})

            prod_weekly_pred['category'] = cat
            prod_monthly_pred['category'] = cat

            # print(prod_monthly_pred.head())
            # print(prod_monthly_pred.head())


            # if cat in ("I", "II", "III", "VII"):
            #     # plot raw weekly data
            #     plot_invoice_data_vs_pred(invoice_data= prod_weekly_raw, pred_data=prod_weekly_pred,dir_name= image_dir,
            #                               cus_no = cus_no, mat_no= mat_no,
            #                               title= "Weekly Aggregated Raw Data vs Prediction"+"(category-"+cat+")")
            #
            #     plot_invoice_data_vs_pred(invoice_data=prod_weekly_clean, pred_data=prod_weekly_pred, dir_name=image_dir,
            #                               cus_no=cus_no, mat_no=mat_no,
            #                               title="Weekly Aggregated Clean Data vs Prediction" + "(category-" + cat + ")")
            #
            #     plot_invoice_data_vs_pred(invoice_data=prod_monthly_raw, pred_data=prod_monthly_pred, dir_name=image_dir,
            #                               cus_no=cus_no, mat_no=mat_no,
            #                               title="Monthly Aggregated Raw Data vs Prediction" + "(category-" + cat + ")")
            #
            #     plot_invoice_data_vs_pred(invoice_data=prod_monthly_clean, pred_data=prod_monthly_pred, dir_name=image_dir,
            #                               cus_no=cus_no, mat_no=mat_no,
            #                               title="Monthly Aggregated Clean Data vs Prediction" + "(category-" + cat + ")")
            #
            # elif cat in ("IV", "V", "VI", "VIII", "IX", "X"):
            #     # plot raw weekly data
            #     plot_invoice_data_vs_pred(invoice_data=prod_monthly_raw, pred_data=prod_monthly_pred, dir_name=image_dir,
            #                               cus_no=cus_no, mat_no=mat_no,
            #                               title="Monthly Aggregated Raw Data vs Prediction" + "(category-" + cat + ")")
            #
            #     plot_invoice_data_vs_pred(invoice_data=prod_monthly_clean, pred_data=prod_monthly_pred, dir_name=image_dir,
            #                               cus_no=cus_no, mat_no=mat_no,
            #                               title="Monthly Aggregated Clean Data vs Prediction" + "(category-" + cat + ")")

            # data creation for error calculation

            _raw_vs_pred_weekly_aggregate = pd.merge(prod_weekly_raw, prod_weekly_pred, how='right',
                                                     on=['customernumber','matnr','date'])
            _all_cus_actual_vs_pred_weekly_aggregate = _all_cus_actual_vs_pred_weekly_aggregate.append(
                _raw_vs_pred_weekly_aggregate)

            _raw_vs_pred_monthly_aggregate = pd.merge(prod_monthly_raw, prod_monthly_pred, how='right',
                                                      on=['customernumber', 'matnr', 'date'])
            _all_cus_actual_vs_pred_monthly_aggregate = _all_cus_actual_vs_pred_monthly_aggregate.append(
                _raw_vs_pred_monthly_aggregate)

        except ValueError:
            print ("Weekly aggregation failed")
            pass
