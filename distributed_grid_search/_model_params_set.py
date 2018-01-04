import distributed_grid_search.properties as p
import distributed_grid_search._sarimax as smax
import itertools
from transform_data.pandas_support_func import *
from transform_data.data_transform import get_weekly_aggregate, get_monthly_aggregate
from properties import *

def generate_all_param_combo_sarimax():
    """
    Generate all parameter combinations for SARIMAX - Weekly.
    :return: Array[Tuple(Tuple, Tuple)]:: Complete parameter set. Structure --> [((p,d,q), (P,D,Q)), ...]
    """
    param_p = xrange(p.p_max + 1)
    param_q = xrange(p.q_max + 1)
    param_d = xrange(p.d_max + 1)

    param_P = xrange(p.P_max + 1)
    param_Q = xrange(p.Q_max + 1)
    param_D = xrange(p.D_max + 1)

    pdq = list(itertools.product(param_p, param_d, param_q))

    seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(param_P, param_D, param_Q))]

    # all_combo = list(itertools.product(pdq, seasonal_pdq))

    all_combo = [((0, 1, 1), (0, 1, 0, 52)),
                 ((0, 0, 1), (1, 1, 0, 52)),
                 ((1, 0, 0), (1, 0, 0, 52)),
                 ((1, 0, 0), (0, 1, 0, 52)),
                 ((1, 1, 1), (0, 1, 0, 52)),
                 ((1, 0, 1), (0, 0, 0, 52)),
                 ((1, 1, 1), (1, 0, 0, 52)),
                 ((1, 0, 0), (0, 0, 0, 52)),
                 ((1, 1, 1), (0, 0, 0, 52)),
                 ((0, 1, 1), (0, 0, 0, 52)),
                 ((1, 1, 0), (0, 1, 0, 52)),
                 ((0, 1, 0), (1, 0, 0, 52)),
                 ((1, 1, 0), (0, 0, 0, 52)),
                 ((1, 1, 0), (1, 0, 0, 52))]

    return all_combo


def generate_models_sarimax(x, **kwargs):
    """
    Generate all the model instances for SARIMAX - Weekly.
    :param x: Tuple(Spark.Row Object, InstanceOf[product_class.products]):: Tuple of data for 1 cust-prod and product_class object assigned to the same
    :param kwargs: OPTIONAL
                    1. 'sep' : Separator used to separate each row element from <Spark.RowObject>.data. Default : '\t'
    :return: Array[Tuple[customernumber, matnr, pdq, seasonal_pqd, data_pd_df_week_aggregated, category_obj]]
    """
    if 'sep' in kwargs.keys():
        sep = kwargs.get('sep')
    else:
        sep = "\t"

    row_object, category_obj = x
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date


    # Unpacking the dataset
    # Extracting only the 0th and 1st element since faced discrepancies in dataset
    data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
                           MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, pdq, seasonal_pqd, data_pd_df_week_aggregated, category_obj) for pdq, seasonal_pqd
            in generate_all_param_combo_sarimax()]


def generate_all_param_combo_prophet():
    """
    Generate all parameter combinations for PROPHET - Weekly.
    :return: Array[{String, String}]:: Complete parameter set. Structure --> [{String: String}, ...]
    """
    import numpy as np

    yearly_seasonality = [True, False]
    seasonality_prior_scale = [{'seasonality_prior_scale': round(i / 10.0, 2)} for i in
                               np.arange(PROPH_W_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_W_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_W_SEASONALITY_PRIOR_SCALE_STEP_SIZE)]

    changepoint_prior_scale = [{'changepoint_prior_scale': int(i)} for i in
                               np.arange(PROPH_W_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_W_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_W_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE)]
    holidays = [{'holidays' : True}, {'holidays' : False}]

    hol_chng_pt_all_combo = [make_single_dict(list(elem)) for elem in list(itertools.product(holidays, changepoint_prior_scale))]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in list(itertools.product(hol_chng_pt_all_combo, yearly_seasonality_all_combo))]

    return _result


def generate_all_param_combo_prophet_monthly():
    """
    Generate all parameter combinations for PROPHET - Monthly.
    :return: Array[{String, String}]:: Complete parameter set. Structure --> [{String: String}, ...]
    """
    import numpy as np

    yearly_seasonality = [True, False]
    seasonality_prior_scale = [{'seasonality_prior_scale': round(i / 10.0, 2)} for i in
                               np.arange(PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_M_SEASONALITY_PRIOR_SCALE_STEP_SIZE)]

    changepoint_prior_scale = [{'changepoint_prior_scale': int(i)} for i in
                               np.arange(PROPH_M_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_M_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_M_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE)]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in list(itertools.product(changepoint_prior_scale, yearly_seasonality_all_combo))]

    return _result


def make_single_dict(a):
    """
    Converts an array of dictionaries into a single one.
    :param a: Array[Dict{elem, elem}]:: Array of dictionaries.
    :return: Dictonary{elem, elem}:: Combined dictionary
    """
    b = {}

    for i in a:
        b.update(i)

    return b


def generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale):
    """
    Generate all combinations of parameter set for yearly_seasonality and seasonality_prior_scale for PROPHET
    :param yearly_seasonality: Array[Boolean]:: Array containing True and/or False
    :param seasonality_prior_scale: Array[Dict{String, Float}]:: Array containing dictionary of 'seasonality_prior_scale' values
                                    Structure:
                                        [{'seasonality_prior_scale': 0.1}, ...]
    :return: Array[Dict{String, Float/String}]:: Array of all parameter combinations
    """
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


def generate_models_prophet(x, **kwargs):
    if 'sep' in kwargs.keys():
        sep = kwargs.get('sep')
    else:
        sep = "\t"

    row_object, category_obj = x
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date


    # Unpacking the dataset
    # Extracting only the 0th and 1st element since faced discrepancies in dataset
    data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
                           MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, data_pd_df_week_aggregated, elem, category_obj) for elem in
            generate_all_param_combo_prophet()]


def generate_models_prophet_monthly(x, **kwargs):
    if 'sep' in kwargs.keys():
        sep = kwargs.get('sep')
    else:
        sep = "\t"

    row_object, category_obj = x
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date


    # Unpacking the dataset
    # Extracting only the 0th and 1st element since faced discrepancies in dataset
    data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
                           MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    # param = {'changepoint_prior_scale': 2, 'yearly_seasonality': True, 'seasonality_prior_scale': 0.2}
    #
    # return [(customernumber, matnr, data_pd_df_week_aggregated, param, category_obj)]

    return [(customernumber, matnr, data_pd_df_week_aggregated, elem, category_obj) for elem in
            generate_all_param_combo_prophet_monthly()]


def generate_all_param_combo_pydlm_monthly():
    """
    CONDITIONS::::

    :return:
    """
    import numpy as np

    trend_degree = [{'trend_degree': int(i)} for i in
                    np.arange(p.trend_degree_low_lim,
                              p.trend_degree_up_lim,
                              p.trend_degree_step_size)]

    trend_w = [{'trend_w': round(i / 10.0, 2)} for i in
                np.arange(p.trend_w_low_lim,
                          p.trend_w_up_lim,
                          p.trend_w_step_size)]

    seasonality_w = [{'seasonality_w': round(i / 10.0, 2)} for i in
                np.arange(p.seasonality_w_low_lim,
                          p.seasonality_w_up_lim,
                          p.seasonality_w_step_size)]

    ar_degree = [{'ar_degree': int(i)} for i in
                    np.arange(p.ar_degree_low_lim,
                              p.ar_degree_up_lim,
                              p.ar_degree_step_size)]

    ar_w = [{'ar_w': round(i / 10.0, 2)} for i in
                np.arange(p.ar_w_low_lim,
                          p.ar_w_up_lim,
                          p.ar_w_step_size)]

    _result = [make_single_dict(i) for i in list(itertools.product(trend_degree, trend_w, seasonality_w,
                                                                   ar_degree, ar_w))]
    return _result

def generate_models_pydlm_monthly(x):
    row_object, category_obj = x
    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual

    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr)

    # Obtaining weeekly aggregate
    data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    return [(customernumber, matnr, data_pd_df_week_aggregated, elem, category_obj) for elem in
            generate_all_param_combo_pydlm_monthly()]

if __name__ == '__main__':
    a = generate_all_param_combo_pydlm_monthly()
    # print (len(a))

    print (generate_all_param_combo_sarimax())
    # param = {'changepoint_prior_scale': 2, 'yearly_seasonality': True, 'seasonality_prior_scale': 0.2}
    #
    # print param
    # print param.get('changepoint_prior_scale')
    # print type(param.get('changepoint_prior_scale'))
    # print param.get('yearly_seasonality')
    # print type(param.get('yearly_seasonality'))
    # print param.get('seasonality_prior_scale')
    # print type(param.get('seasonality_prior_scale'))
    #
    # # print [(1, elem)for elem in generate_all_param_combo_prophet_monthly()]
    # for i in a:
    #     print i
    #     print i.get('changepoint_prior_scale')
    #     print type(i.get('changepoint_prior_scale'))
    #     print i.get('yearly_seasonality')
    #     print type(i.get('yearly_seasonality'))
    #     print i.get('seasonality_prior_scale')
    #     print type(i.get('seasonality_prior_scale'))
