import pandas as pd
import numpy as np
from transform_data.data_transform import *
from statsmodels.stats import diagnostic as diag
import matplotlib.pylab as plt
from dateutil import parser
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 15,6
import os

# data load
file_dir = "C:\\files\\CONA_Conv_Store_Data\\"

image_dir = "C:\\files\\CONA_Conv_Store_Data\\temp\\white_noise_test\\FL_TS\\"


raw_data = pd.read_csv(file_dir + "invoice_data_raw_cat_123_FL_100_cutoff_dt_10-11-2017.tsv", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

# cus_no = 500124803
# mat_no = 115583

################################ -- Functions -- ################################
def save_plot(x, y, xlable, ylable, title, dir_name, cus_no, mat_no):
    fig = plt.figure()
    plt.plot(x, y, marker="*", markerfacecolor="red", markeredgecolor="red", markersize=3.0)
    plt.title(title)
    plt.xlabel(xlable)
    plt.ylabel(ylable)
    plt.legend()

    save_file = os.path.join(dir_name, str(cus_no) + "_" + str(mat_no) + "_" + title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)
#################################################################################


white_noise_test_df = pd.DataFrame(columns= ["cus", "mat", "wnt_status"])

# cus = raw_data[raw_data.customernumber == cus_no]
# prod = cus[cus.matnr == mat_no]

# print(white_noise_test_df.head())

for cus_no in raw_data.customernumber.unique():
    cus = raw_data[raw_data.customernumber == cus_no]
    for mat_no in cus.matnr.unique():
        prod = cus[cus.matnr == mat_no]

        if len(prod) > 208:
                # print(prod.dtypes)

            prod[["quantity", "q_indep_p"]] = prod[["quantity", "q_indep_p"]].apply(lambda x: x.astype("float"))

            prod = get_weekly_aggregate(inputDF=prod)

            # prod = get_monthly_aggregate(inputDF=prod)

            prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
            prod = prod[['ds', 'y']]
            prod.ds = prod.ds.apply(str).apply(parser.parse)
            prod.y = prod.y.apply(float)
            prod = prod.sort_values('ds')
            prod = prod.reset_index(drop=True)
            prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

            # print(prod.head())

            x = np.array(prod['y']).astype(float)

            try:
                lj_box_test = diag.acorr_ljungbox(x, lags=104, boxpierce=False)

                min_p_val = min(lj_box_test[1])

                if min_p_val < 0.05:
                    ts_type = "Not-White-Noise"
                else:
                    ts_type = "White-Noise"

                cus_wnt_status = pd.DataFrame(data={'cus': [cus_no], 'mat': [mat_no], 'wnt_status': [ts_type]})

                white_noise_test_df = white_noise_test_df.append(cus_wnt_status)
                # print(white_noise_test_df.head())

                # 2d Image saver function
                save_plot(x= prod.ds, y= prod.y, xlable= "date", ylable= "quantity", title= ts_type + "_W-" + "(p-value = " +str(min_p_val) + ")",
                          dir_name= image_dir, cus_no= cus_no, mat_no= mat_no)

            except ValueError:
                print("Test Failed")

white_noise_test_df.to_csv("C:\\files\\CONA_Conv_Store_Data\\temp\\white_noise_test\\white_noise_test_df_FL_W.csv")


