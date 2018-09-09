from transform_data.data_transform import *
from model.ma_outlier import *
from distributed_grid_search._sarimax import *
from distributed_grid_search._sarimax_monthly import *
import pandas as pd
from dateutil import parser
from matplotlib.pylab import rcParams
import time
from order_compare.od_com_support_func import *

rcParams['figure.figsize'] = 15, 6

########################
route_id_list = ['CC002189']
########################

# data load and transform
file_dir = "C:\\CONA_CSO\\thadeus_route\\raw_data\\"

cv_result_dir = "C:\\CONA_CSO\\thadeus_route\\cv_result\\"

order_compare_dir = "C:\\CONA_CSO\\thadeus_route\\compare\\"

vl_dir = "C:\\CONA_CSO\\thadeus_route\\vl\\"

# image save folder
image_dir = "C:\\CONA_CSO\\thadeus_route\\model_fit_plots\\temp\\"

order_compare = pd.read_csv(order_compare_dir + "compare_2018-08-09.tsv", sep= "\t", header=None,
                            names=["custmernumber", "mat_no", "order_date", "actual_q", "pred_q", "dd_actual",
                                   "dd_pred", "month"])

print("Order compare data:\n")
print(order_compare.head())

##############################################
# obtaining customer list for the given route
###############################################
vl = pd.read_csv(vl_dir + "AZ_TCAS_VL.csv",sep=",", header=0, encoding = "ISO-8859-1")

# print("visit list data:\n")
# print(vl.head())
# print(vl.columns.values)

vl_select_route = vl.loc[vl['USERID'].isin(route_id_list)]

# print("visit list selected customers:\n")
# print(vl_select_route.head())

customer_list = list(set(vl_select_route['KUNNR'].values))
# print(customer_list)
################################################

# filter compare result
order_comp_select_cust = order_compare.loc[(order_compare['custmernumber'].isin(customer_list)) &
                                           ((order_compare['order_date']>= '2018-07-02') &
                                            (order_compare['order_date']<= '2018-07-27'))]
print("final orderdate compare data:\n")
print(order_comp_select_cust.head())
################################################



#orderate basis comparison
od_order_comp = order_comp_select_cust.copy()
od_order_comp['q_diff'] = abs(order_comp_select_cust['actual_q'] - order_comp_select_cust['pred_q'])

print(od_order_comp.head())

plot_count_hist(data=od_order_comp, field= 'q_diff', title='Histogram of Error Quantity on Order Date Basis',
                num_bar=10, image_dir=image_dir)

ax =od_order_comp['q_diff'].value_counts().sort_index().plot(kind = 'bar',grid=True, color='#607c8e')
plt.title('Histogram of Error Quantity on Order Date Basis')
plt.xlabel('Absolute Error Quantity(cs)')
plt.ylabel('Count')
plt.grid(axis='y', alpha=0.75)
# create a list to collect the plt.patches data
totals = []

# find the values and append to list
for i in ax.patches:
    totals.append(i.get_height())

# set individual bar lables using above list
total = sum(totals)

for i in ax.patches[:11]:
    # get_x pulls left or right; get_height pushes up or down
    ax.text(i.get_x()-.03, i.get_height()+ 5.0, \
            str(round((i.get_height()/total)*100, 2))+'%', fontsize=13,
                color='dimgrey')
ax.set_xlim(left=None, right= 10.5)
plt.show()
################################################








