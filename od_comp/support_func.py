import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import os
import time
rcParams['figure.figsize'] = 15, 6

def plot_count_hist(data, field, title, x_label, num_bar, x_lim, image_dir):
    """
    plot the histogram of count
    :param data: df
    :param field: feature name(string)
    :param num_bar: bar count
    :param image_dir: dir name
    :return: none
    """
    fig = plt.figure()
    # print(data[field].value_counts().sort_index())
    ax =data[field].value_counts().sort_index().plot(kind = 'bar',grid=True, color='#607c8e')
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel('Count')
    plt.grid(axis='y', alpha=0.75)
    # create a list to collect the plt.patches data
    totals = []

    # find the values and append to list
    for i in ax.patches:
        totals.append(i.get_height())

    # set individual bar lables using above list
    total = sum(totals)

    for i in ax.patches[:(num_bar + 1)]:
        # get_x pulls left or right; get_height pushes up or down
        ax.text(i.get_x(), i.get_height()+ 0.1,\
                str(round((i.get_height()/total)*100, 2))+'%', fontsize=13,
                    color='dimgrey')
    ax.set_xlim(left=None, right= x_lim)
    save_file = os.path.join(image_dir, title + ".png")
    plt.savefig(save_file, bbox_inches='tight')
    plt.close(fig)

def insert_missing_dates(data):

    import pandas as pd
    import parser

    od_order_comp = data.copy()

    colnames = list(od_order_comp)

    od_order_comp['order_date'] = od_order_comp['order_date'].astype(str)
    cust_date = od_order_comp.groupby(['customernumber']).apply(lambda x: x['order_date'].unique())
    cust_mat_date = od_order_comp.groupby(['customernumber', 'mat_no']). \
        apply(lambda x: list(set(cust_date.loc[cust_date.index.isin(x['customernumber'].values)].values[0]).
                             difference(set(x['order_date'].values)))).rename('date_list').reset_index()

    missing_od_df = cust_mat_date.apply(lambda x: pd.Series(x['date_list']), axis=1).stack().reset_index(level=1,
                                                                                                         drop=True)
    missing_od_df.name = "order_date"
    missing_od_df = cust_mat_date.drop('date_list', axis=1).join(missing_od_df).reset_index(drop=True)
    other_cols = dict.fromkeys(list(set(colnames).difference(set(list(missing_od_df)))), 0)
    missing_od_df = missing_od_df.assign(**other_cols)
    od_order_comp_with_zeros = od_order_comp.append(missing_od_df).reset_index(drop=True)

    return od_order_comp_with_zeros

def filter_mismatch_dates(data):

    import pandas as pd

    od_order_comp_with_zeros = data.copy()

    od_order_comp_with_zeros['order_date'] = od_order_comp_with_zeros['order_date'].astype(str)

    cus_od_agg = od_order_comp_with_zeros.groupby(['customernumber', 'order_date'])[['actual_q', 'pred_q']]. \
        sum().reset_index()
    cus_od_agg = cus_od_agg.loc[(cus_od_agg['actual_q'] != 0.0) & (cus_od_agg['pred_q'] != 0.0)]
    cus_od_agg['cus_od'] = cus_od_agg['customernumber'].map(str) + "_" + cus_od_agg['order_date']

    od_order_comp_with_zeros['cus_od'] = od_order_comp_with_zeros['customernumber'].map(str) + "_" + \
                                         od_order_comp_with_zeros['order_date']

    # print("printing order dates with zeros length:\n")
    # print(len(od_order_comp_with_zeros))

    od_order_comp_with_zeros_cleaned = od_order_comp_with_zeros.loc[
        od_order_comp_with_zeros['cus_od'].isin(cus_od_agg['cus_od'].values)]

    # print(od_order_comp_with_zeros_cleaned.head())

    od_order_comp_with_zeros_cleaned_final = od_order_comp_with_zeros_cleaned.drop('cus_od', axis=1)

    return od_order_comp_with_zeros_cleaned_final

def perc_diff_bucket(num):

    if num < -100:
        diff_bucket = "[a]Below[-100]"
    elif num < -50:
        diff_bucket = "[b][-100,-50)"
    elif num < -25:
        diff_bucket = "[c][-50,-25)"
    elif num < 0:
        diff_bucket = "[d][-25,0)"
    elif num == 0:
        diff_bucket = "[e][0]"
    elif num <= 25:
        diff_bucket = "[f](0,25]"
    elif num <= 50:
        diff_bucket = "[g](25,50]"
    elif num <= 100:
        diff_bucket = "[h](50,100]"
    else:
        diff_bucket = "[i]Above[100]"

    return  diff_bucket


