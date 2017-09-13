import distributed_grid_search.properties as p
import distributed_grid_search._sarimax as smax
import itertools
from transform_data.pandas_support_func import *
from transform_data.data_transform import get_weekly_aggregate

def generate_all_param_combo_sarimax():
    param_p = xrange(p.p_max + 1)
    param_q = xrange(p.q_max + 1)
    param_d = xrange(p.d_max + 1)

    param_P = xrange(p.P_max + 1)
    param_Q = xrange(p.Q_max + 1)
    param_D = xrange(p.D_max + 1)

    pdq = list(itertools.product(param_p, param_d, param_q))

    seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(param_P, param_D, param_Q))]

    all_combo = list(itertools.product(pdq, seasonal_pdq))

    return all_combo


def generate_models(row_object):
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual


    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, pdq, seasonal_pqd, data_pd_df_week_aggregated) for pdq, seasonal_pqd in generate_all_param_combo_sarimax()]
