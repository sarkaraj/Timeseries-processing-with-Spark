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

pdt_cat_dir = "C:\\CONA_CSO\\thadeus_route\\pdt_cat\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\outlier_removal\\"

raw_data = pd.read_csv(file_dir + "raw_invoices_2018-07-06.tsv",
                       sep="\t", header=None, names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

pdt_cat_123 = pd.read_csv(pdt_cat_dir + "cat_123_2018-07-30.tsv",
                          sep= "\t", header=None, names= ["customernumber", "matnr", "cat"])

pdt_cat_456 = pd.read_csv(pdt_cat_dir + "cat_456_2018-07-30.tsv",
                          sep= "\t", header=None, names= ["customernumber", "matnr", "cat"])

pdt_cat_7 = pd.read_csv(pdt_cat_dir + "cat_7_2018-07-30.tsv",
                        sep= "\t", header=None, names= ["customernumber", "matnr", "cat"])

pdt_cat_8910 = pd.read_csv(pdt_cat_dir + "cat_8910_2018-07-30.tsv",
                           sep= "\t", header=None, names= ["customernumber", "matnr", "cat"])

pdt_cat = pd.concat([pdt_cat_123, pdt_cat_456, pdt_cat_7, pdt_cat_8910])


print("Raw Data Head:\n")
print(raw_data.head())

print("pdt_cat data:\n")
print(pdt_cat.head())
print(len(pdt_cat))

raw_data['customernumber'] = raw_data['customernumber'].astype(str)
pdt_cat['customernumber'] = pdt_cat['customernumber'].astype(str)

for i in range(len(pdt_cat)):

    cus_no = pdt_cat['customernumber'].values[i]
    mat_no = pdt_cat['matnr'].values[i]
    cat = pdt_cat['cat'].values[i]

    # print(type(cus_no))
    # print(mat_no)
    # print(cat)

    ## for weekly it has to be sunday, monthly last dte of month
    mdl_cutoff_date = parser.parse("2018-07-01")  # "2018-06-03"

    # filtering data

    print((raw_data['customernumber'].values))

    if cus_no in (raw_data['customernumber'].values):
        print("customer is available.\n")
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

        window = 12
        plot = False

        if cat in ["I", "II", "III", "VII"]:
            window = 12
            if (len(data_w_agg)) > 26:
                data_w_agg_cleaned = ma_replace_outlier(data=data_w_agg, n_pass=3, aggressive=True,
                                                        sigma=4, window_size=window)  # initially sigma was 2.5
                plot = True
        elif cat in ["IV", "V", "VI", "VIII", "IX"]:
            window = 18
            if (len(data_w_agg)) > 26:
                data_w_agg_cleaned = ma_replace_outlier(data=data_w_agg, n_pass=3, aggressive=True,
                                                        sigma=5, window_size=window)  # initially sigma was 2.5
                plot = True
        elif cat in ["X"]:
            # window = 18
            # if (len(data_w_agg)) < 26:
            #     data_w_agg_cleaned = ma_replace_outlier(data=data_w_agg, n_pass=3, aggressive=True,
            #                                             sigma=5, window_size=window)  # initially sigma was 2.5
            plot = False

        if plot == True:
            # data_w_agg_cleaned = ma_replace_outlier(data=data_w_agg, n_pass=3, aggressive=True,
            #                                     sigma=4, window_size= window)  # initially sigma was 2.5


            two_dim_save_plot(x1=raw_w_agg_data.ds, y1=raw_w_agg_data.y, y1_label="Raw_data",
                          x2=data_w_agg_cleaned.ds, y2=data_w_agg_cleaned.y, y2_label="Cleaned_data",
                          xlable="Date", ylable="Quantity",
                          title="Raw_vs_Cleaned_Data" + "_" + str(cat), cus_no=cus_no, mat_no=mat_no, dir_name=image_dir)





