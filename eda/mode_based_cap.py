import pandas as pd
import numpy as np
from dateutil import parser
import datetime as DT
today = DT.date.today()
week_ago = today - DT.timedelta(days=7)

# current_date = "20180921"
# # current_date = DT.date.today()
#
file_dir = "C:/CONA_CSO/overall/raw_data/"

od_comp_file = "order_compare_2018-09-24.tsv"

order_compare_dir = "C:\\CONA_CSO\\thadeus_route\\compare\\"
#
# raw_data = pd.read_csv(file_dir + "invoice_data3.csv", sep = ",", header = 1,
#                        names = ["customernumber", "matnr", "del_date", "quantity"])
#
# raw_data["del_date"] = raw_data["del_date"].astype(str).apply(parser.parse)
#
# print(len(raw_data))
#
# raw_data_6month = raw_data.loc[raw_data['del_date'] > (parser.parse(current_date) - DT.timedelta(days=180))]
#
# raw_data_6month.to_csv(file_dir + "raw_data_6month.csv", index = False)

raw_data_6month = pd.read_csv(file_dir + "raw_data_6month.csv")
raw_data_6month.rename(columns={'matnr': 'mat_no'}, inplace=True)
# raw_data_6month["del_date"] = raw_data_6month["del_date"].astype(str).apply(parser.parse)


order_compare = pd.read_csv(order_compare_dir + od_comp_file, sep="\t",
                                header=None,
                                names=["customernumber", "mat_no", "order_date", "actual_q", "pred_q", "dd_actual",
                                       "dd_pred", "month", "pred_dec", "cap", "prod_desc", "fridge"])

order_compare_latest = order_compare.loc[order_compare.groupby(['customernumber',
                                                                'mat_no']).apply(lambda x: pd.to_datetime(x['order_date']).idxmax())]

# print(order_compare.dtypes)
# print(order_compare.head())
# print(len(order_compare))
#
# print(len(order_compare_latest))

raw_data_with_cap = pd.merge(left=raw_data_6month, right= order_compare_latest, how='inner', left_on=['customernumber', 'mat_no'],
                             right_on=['customernumber', 'mat_no'])

final_data = raw_data_with_cap.loc[:,['customernumber', 'mat_no', 'del_date', 'quantity','cap']]

final_data['quantity'] = final_data['quantity'].astype(int)

final_data['cap'] = pd.to_numeric(final_data['cap'], errors='coerce')

final_data = final_data.loc[~final_data['cap'].isnull()]

final_data = final_data.loc[final_data['quantity'] > 0]

mat_15pack = [133116, 153190, 153347, 134275, 152221, 152222, 156071, 133115, 134608, 153215, 155070]

final_data = final_data.loc[final_data['mat_no'].isin(mat_15pack)]



final_data['overcap_order'] = final_data['quantity'].astype(int) - np.ceil(final_data['cap'].astype(int)/2.5)

print(final_data.head(50))
print(final_data.info())
print(len(final_data))


print(final_data.loc[final_data['overcap_order'] > 0]['overcap_order'].value_counts(sort = True))
print(final_data.loc[final_data['overcap_order'] > 0]['overcap_order'].value_counts(sort = True).sum())

# print(final_data['quantity'].value_counts())
print("\n####################################")
order_compare = order_compare.loc[order_compare['mat_no'].isin(mat_15pack)]
order_compare = order_compare.loc[order_compare['actual_q']>0]
order_compare['overcap_order'] = order_compare['actual_q'].astype(int) - np.ceil(pd.to_numeric(order_compare['cap'], errors='coerce')/2.5)

order_compare.to_csv(file_dir + "15pk_orver_predict.csv", index = False)

print(order_compare.head(50))
print(order_compare.info())
print(len(order_compare))


print(order_compare.loc[order_compare['overcap_order'] > 0]['overcap_order'].value_counts(sort = True))
print(order_compare.loc[order_compare['overcap_order'] > 0]['overcap_order'].value_counts(sort = True).sum())


print(order_compare.loc[order_compare['overcap_order'] > 0]['overcap_order'].value_counts(sort = True).index.values)