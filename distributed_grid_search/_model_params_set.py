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


def generate_models_sarimax(row_object):
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual


    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, pdq, seasonal_pqd, data_pd_df_week_aggregated) for pdq, seasonal_pqd in generate_all_param_combo_sarimax()]


def generate_all_param_combo_prophet():
    """
    CONDITIONS::::
    yearly_seasonality = True / False
    seasonality_prior_scale
    {0.1, 0.2, _______, 1)

    ###########################

    changepoint_prior_scale
    {1, 2________, 10}

    ###########################

    Holidays = Holidays / None

    :return:
    """
    import numpy as np

    yearly_seasonality = [True, False]
    seasonality_prior_scale = [{'seasonality_prior_scale': i} for i in np.arange(0.1, 0.7, 0.2)]

    changepoint_prior_scale = [{'changepoint_prior_scale': i} for i in np.arange(1, 6, 2)]
    holidays = [{'holidays' : True}, {'holidays' : False}]

    hol_chng_pt_all_combo = [make_single_dict(list(elem)) for elem in list(itertools.product(holidays, changepoint_prior_scale))]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in list(itertools.product(hol_chng_pt_all_combo, yearly_seasonality_all_combo))]

    return _result

def generate_all_param_combo_prophet_monthly():
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
    seasonality_prior_scale = [{'seasonality_prior_scale': i} for i in np.arange(0.1, 0.7, 0.2)]

    changepoint_prior_scale = [{'changepoint_prior_scale': i} for i in np.arange(1, 6, 2)]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in list(itertools.product(changepoint_prior_scale, yearly_seasonality_all_combo))]

    return _result


def make_single_dict(a):
    b = {}

    for i in a:
        b.update(i)

    return b


def generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale):
    yearly_seasonality_all_combo = []

    for i in yearly_seasonality:
        if i == True:
            for elem in seasonality_prior_scale:
                d2 = {}
                d1 = {'yearly_seasonality': True}
                d2.update(d1)
                d2.update(elem)
                yearly_seasonality_all_combo.append(d2)

        else:
            yearly_seasonality_all_combo.append({'yearly_seasonality': i})

    return yearly_seasonality_all_combo



def generate_models_prophet(row_object):
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual


    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, data_pd_df_week_aggregated, elem) for elem in generate_all_param_combo_prophet()]

def generate_models_prophet_monthly(row_object):
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual


    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, data_pd_df_week_aggregated, elem) for elem in
            generate_all_param_combo_prophet_monthly()]


# print len(generate_all_param_combo_prophet())
# print generate_all_param_combo_prophet()
# make_single_dict()
# print(generate_all_param_combo_prophet_monthly())
