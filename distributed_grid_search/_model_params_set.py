import distributed_grid_search.properties as p
import distributed_grid_search._sarimax as smax
import itertools
from transform_data.pandas_support_func import *
from transform_data.data_transform import get_weekly_aggregate, get_monthly_aggregate
from distributed_grid_search.properties import *
import numpy as np


def generate_all_param_combo_sarimax(**kwargs):
    from math import ceil

    # TODO: Uncomment the below line
    # param_p = range(p.p_Weekly_lower_limit, p.p_Weekly_upper_limit)
    
    param_p = [0, 2, 3]
    param_q = range(p.q_Weekly_lower_limit, p.q_Weekly_upper_limit)
    param_d = range(p.d_Weekly_lower_limit, p.d_Weekly_upper_limit)

    if "category" in kwargs.keys():
        category = kwargs.get("category")

        if category == "I":
            param_P = range(p.P_max + 1)
            param_Q = range(p.Q_max + 1)
            param_D = range(p.D_max + 1)

        else:
            param_P = [0]
            param_Q = [0]
            param_D = [0]
    else:
        param_P = range(p.P_max + 1)
        param_Q = range(p.Q_max + 1)
        param_D = range(p.D_max + 1)

    # TODO: testing with (0,1,1) and (0,2,2)
    # pdq = list(itertools.product(param_p, param_d, param_q))
    pdq = [(0, 1, 1), (0, 2, 2), (0, 1, 2)]

    seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(param_P, param_D, param_Q))]

    all_combo = list(itertools.product(pdq, seasonal_pdq))

    if p.ENABLE_SAMPLING:
        sample_size = int(ceil(float(len(all_combo)) * (float(p.GRID_SEARCH_SAMPLING_SAMPLE_SIZE_WEEKLY) / 100.0)))
        index_of_samples = np.random.choice(range(len(all_combo)), size=sample_size, replace=False)
        sampled_combo = [all_combo[elem] for elem in index_of_samples]

        return sampled_combo
    else:
        return all_combo


def generate_models_sarimax(x, **kwargs):
    # if 'sep' in kwargs.keys():
    #     sep = kwargs.get('sep')
    # else:
    #     sep = "\t"

    # row_object, category_obj = x
    # customernumber = row_object.customernumber
    # matnr = row_object.matnr
    # MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date
    #
    # # Unpacking the dataset
    # # Extracting only the 0th and 1st element since faced discrepancies in dataset
    # data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    # data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
    #                        MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)
    #
    # # Obtaining weeekly aggregate
    # data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)

    customernumber = x[0]
    matnr = x[1]
    data_pd_df_week_aggregated = x[2]
    revised_cat_object = x[3]
    post_outlier_period_flag = x[4]

    return [(customernumber, matnr, pdq, seasonal_pqd, data_pd_df_week_aggregated, revised_cat_object, post_outlier_period_flag) for pdq, seasonal_pqd
            in generate_all_param_combo_sarimax(category=revised_cat_object.category)]


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
    seasonality_prior_scale = [{'seasonality_prior_scale': round(i / 10.0, 2)} for i in
                               np.arange(PROPH_W_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_W_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_W_SEASONALITY_PRIOR_SCALE_STEP_SIZE)]

    changepoint_prior_scale = [{'changepoint_prior_scale': int(i)} for i in
                               np.arange(PROPH_W_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_W_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_W_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE)]
    holidays = [{'holidays': True}, {'holidays': False}]

    hol_chng_pt_all_combo = [make_single_dict(list(elem)) for elem in
                             list(itertools.product(holidays, changepoint_prior_scale))]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in
               list(itertools.product(hol_chng_pt_all_combo, yearly_seasonality_all_combo))]

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
    seasonality_prior_scale = [{'seasonality_prior_scale': round(i / 10.0, 2)} for i in
                               np.arange(PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_M_SEASONALITY_PRIOR_SCALE_STEP_SIZE)]

    # TODO: Added to test model improvement, can be removed later
    optional_seeasonality_prior_scale = [{'seasonality_prior_scale': 0.05}]
    seasonality_prior_scale = seasonality_prior_scale + optional_seeasonality_prior_scale

    changepoint_prior_scale = [{'changepoint_prior_scale': int(i)} for i in
                               np.arange(PROPH_M_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT,
                                         PROPH_M_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT,
                                         PROPH_M_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE)]

    yearly_seasonality_all_combo = generate_all_yearly_seasonality_params(yearly_seasonality, seasonality_prior_scale)

    _result = [make_single_dict(i) for i in
               list(itertools.product(changepoint_prior_scale, yearly_seasonality_all_combo))]

    return _result


def generate_all_param_combo_sarimax_monthly(**kwargs):
    param_p = range(p.p_Monthly_lower_limit, p.p_Monthly_upper_limit)
    param_q = range(p.q_Monthly_lower_limit, p.q_Monthly_upper_limit)
    param_d = range(p.d_Monthly_lower_limit, p.d_Monthly_upper_limit)

    if "category" in kwargs.keys():
        category = kwargs.get("category")

        if category == "IV":
            param_P = range(p.P_max_M + 1)
            param_Q = range(p.Q_max_M + 1)
            param_D = range(p.D_max_M + 1)

        elif category == "V":
            param_P = [0]
            param_Q = [0]
            param_D = [0]
    else:
        param_P = range(p.P_max_M + 1)
        param_Q = range(p.Q_max_M + 1)
        param_D = range(p.D_max_M + 1)


    trend_c = range(p.c_max_M +1)
    trend_c_t = range(p.c_t_max_M +1)
    trend_c_tsqaure = range(p.c_tsquare_max_M +1)

    pdq = list(itertools.product(param_p, param_d, param_q))

    seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(param_P, param_D, param_Q))]
    
    trend = [[x[0], x[1], x[2]] for x in list(itertools.product(trend_c, trend_c_t, trend_c_tsqaure))]

    all_combo = list(itertools.product(pdq, seasonal_pdq, trend))
    return all_combo


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


def generate_models_sarimax_monthly(x, **kwargs):
    # if 'sep' in kwargs.keys():
    #     sep = kwargs.get('sep')
    # else:
    #     sep = "\t"
    #
    # row_object, category_obj = x
    # customernumber = row_object.customernumber
    # matnr = row_object.matnr
    # MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')  # # is of type datetime.date
    #
    # # Unpacking the dataset
    # # Extracting only the 0th and 1st element since faced discrepancies in dataset
    # data_array = [[row.split(sep)[0], row.split(sep)[1]] for row in row_object.data]
    # data_pd_df = get_pd_df(data_array=data_array, customernumber=customernumber, matnr=matnr,
    #                        MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE)
    #
    # # Obtaining weeekly aggregate
    # data_pd_df_week_aggregated = get_weekly_aggregate(data_pd_df)
    customernumber = x[0]
    matnr = x[1]
    data_pd_df_month_aggregated = x[2]
    revised_cat_object = x[3]

    return [(customernumber, matnr, pdq, seasonal_pqd, trend, data_pd_df_month_aggregated, revised_cat_object) for pdq, seasonal_pqd, trend
            in generate_all_param_combo_sarimax_monthly(category=revised_cat_object.category)]


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
    print (generate_all_param_combo_sarimax(category="II"))
    print (len((generate_all_param_combo_sarimax_monthly(category="V"))))
