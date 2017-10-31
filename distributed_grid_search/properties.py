################WEEKLY################
min_train_days=731
test_points=2
pred_points=2


################MONTHLY################
test_points_monthly=1
pred_points_monthly=2

################
p_max=1
d_max=1
q_max=1

P_max=1
Q_max=1
D_max=1

#################
################WEEKLY################################WEEKLY################################WEEKLY################
#################SARIMAX##################################
# OPTIONS::: 'rmse', 'mape', 'wre_med_6', 'wre_max_6', 'wre_mean_6', 'wre_med_12', 'wre_max_12', 'wre_mean_12', 'cum_error'
SARIMAX_W_MODEL_SELECTION_CRITERIA = 'wre_med_12'

#################
################WEEKLY################################WEEKLY################################WEEKLY################
#  PROPHET MONTHLY PARAMETERS - Actual LOWER_LIMIT and UPPER_LIMIT is PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT / 10.0
#  AND PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT / 10.0 respectively

PROPH_W_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT = 1  # # Point is INCLUSIVE
PROPH_W_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT = 6  # # Point is EXCLUSIVE
PROPH_W_SEASONALITY_PRIOR_SCALE_STEP_SIZE = 2

PROPH_W_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT = 1  # # Point is INCLUSIVE
PROPH_W_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT = 6  # # Point is EXCLUSIVE
PROPH_W_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE = 1

# OPTIONS::: 'rmse', 'mape', 'wre_med_6', 'wre_max_6', 'wre_mean_6', 'wre_med_12', 'wre_max_12', 'wre_mean_12', 'cum_error'
PROPH_W_MODEL_SELECTION_CRITERIA = 'wre_med_12'

################MONTHLY################################MONTHLY################################MONTHLY################


################MONTHLY################################MONTHLY################################MONTHLY################
#  PROPHET MONTHLY PARAMETERS - Actual LOWER_LIMIT and UPPER_LIMIT is PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT / 10.0
#  AND PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT / 10.0 respectively

PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT = 1  # # Point is INCLUSIVE
PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT = 4  # # Point is EXCLUSIVE
PROPH_M_SEASONALITY_PRIOR_SCALE_STEP_SIZE = 1

PROPH_M_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT = 1  # # Point is INCLUSIVE
PROPH_M_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT = 4  # # Point is EXCLUSIVE
PROPH_M_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE = 1

# OPTIONS::: 'rmse', 'mape', 'mre_med_3', 'mre_max_3', 'mre_mean_3', 'mre_med_4', 'mre_max_4', 'mre_mean_4', 'cum_error'
PROPH_M_MODEL_SELECTION_CRITERIA = 'mre_med_4'

################MONTHLY################################MONTHLY################################MONTHLY################

#################
################WEEKLY################################WEEKLY################################WEEKLY################
#  PYDLM MONTHLY PARAMETERS - Actual LOWER_LIMIT and UPPER_LIMIT for wts is wt / 10.0

trend_degree_low_lim = 1  # # Point is INCLUSIVE
trend_degree_up_lim = 3  # # Point is EXCLUSIVE
trend_degree_step_size = 1

trend_w_low_lim = 5  # # Point is INCLUSIVE
trend_w_up_lim = 11  # # Point is EXCLUSIVE
trend_w_step_size = 5

seasonality_w_low_lim = 5  # # Point is INCLUSIVE
seasonality_w_up_lim = 11  # # Point is EXCLUSIVE
seasonality_w_step_size = 5

ar_degree_low_lim = 1  # # Point is INCLUSIVE
ar_degree_up_lim = 4  # # Point is EXCLUSIVE
ar_degree_step_size = 1

ar_w_low_lim = 5  # # Point is INCLUSIVE
ar_w_up_lim = 11  # # Point is EXCLUSIVE
ar_w_step_size = 5

# OPTIONS::: 'rmse', 'mape', 'wre_med_6', 'wre_max_6', 'wre_med_12', 'wre_max_12', 'cum_error'
PYDLM_M_MODEL_SELECTION_CRITERIA = 'mre_max_4'

################MONTHLY################################MONTHLY################################MONTHLY################
# if __name__ == "__main__":
#     import numpy as np
#
#     a = np.arange(PROPH_M_SEASONALITY_PRIOR_SCALE_LOWER_LIMIT,
#                   PROPH_M_SEASONALITY_PRIOR_SCALE_UPPER_LIMIT,
#                   PROPH_M_SEASONALITY_PRIOR_SCALE_STEP_SIZE)
#
#     print a
#
#     for i in a:
#         print round(i / 10.0, 2)
#
#     b = np.arange(PROPH_M_CHANGEPOINT_PRIOR_SCALE_LOWER_LIMIT,
#                   PROPH_M_CHANGEPOINT_PRIOR_SCALE_UPPER_LIMIT,
#                   PROPH_M_CHANGEPOINT_PRIOR_SCALE_STEP_SIZE)
#
#     # for i in b:
#     #     print i
