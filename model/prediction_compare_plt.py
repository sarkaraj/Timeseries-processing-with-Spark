from transform_data.pandas_support_func import *
import pandas as pd
from matplotlib.pylab import rcParams
from dateutil import parser
from model.save_images import *
from model.plt_data import *

rcParams['figure.figsize'] = 15, 6

# data load and transform
file_dir = "C:\\files\\CONA_Conv_Store_Data\\CCBF\\raw_data\\"

# image save folder
image_dir = "C:\\files\\CONA_Conv_Store_Data\\CCBF\\raw_data\\plots\\"

raw_data = pd.read_csv(file_dir + "raw_invoice_39Stores_CCBF_COMPLETE.tsv", sep="\t", header=None,
                       names=['customernumber', 'matnr', 'date', 'quantity', 'q_indep_p'])

prediction = pd.read_csv(file_dir + "predicted_invoice_with_39Stores_CCBF_JULY_TO_NOV.tsv", sep= "\t", header= None,
                         names= ['customernumber', 'matnr', 'order_date', 'delivery_date', 'cso_quantity', 'decimal',
                                 'pred_quantity', 'pred_description'])

prediction['pred_description'] = prediction['pred_description'].apply(lambda x : convert_string_to_array_of_dict(x))

for cus_no in prediction.customernumber.unique():
    cus_raw = raw_data[raw_data.customernumber == cus_no]
    cus_pred = prediction[prediction.customernumber == cus_no]
    for mat_no in cus_pred.matnr.unique():
        prod_raw = cus_raw[cus_raw.matnr == mat_no]
        prod_pred = cus_pred[cus_pred.matnr == mat_no]

        prod_raw.date = prod_raw.date.apply(str).apply(parser.parse)
        prod_raw.quantity = prod_raw.quantity.apply(float)
        prod_raw = prod_raw.sort_values('date')
        prod_raw = prod_raw.reset_index(drop=True)
        # print(len(prod_raw))

        prod_pred.delivery_date = prod_pred.delivery_date.apply(str).apply(parser.parse)
        prod_pred.cso_quantity = prod_pred.cso_quantity.apply(float)
        prod_pred = prod_pred.sort_values('delivery_date')
        prod_pred = prod_pred.reset_index(drop=True)
        # print(len(prod_pred))

        plot_raw_data_pred(invoice_data=prod_raw, pred_data=prod_pred,dir_name= image_dir,
                           cus_no= cus_no, mat_no= mat_no)


dominant_cat = find_dominant_category(data = prediction, cus_no= 500124803, mat_no= 103029)

print(dominant_cat)