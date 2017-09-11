from model.weekly_model import weekly_ensm_model
from transform_data.data_transform import get_weekly_aggregate
from transform_data.pandas_support_func import *


def model_fit(row_object):
    import pandas as pd

    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual


    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = pd.DataFrame(data_array, columns=['date', 'quantity', 'q_indep_p']).convert_objects(convert_numeric=True)
    data_pd_df['customernumber'] = customernumber
    data_pd_df['matnr'] = matnr

    # Obtaining weeekly aggregate
    data_pd_df = get_weekly_aggregate(data_pd_df)
    # running weekly ensemble model
    output = weekly_ensm_model(prod=data_pd_df, cus_no=customernumber, mat_no=matnr)
    # converting dataframe to list for ease of handling
    output_rdd_row = extract_from_dict(output.to_dict(orient='index'))

    return output_rdd_row

    # return data_pd_df['quantity'], data_pd_df['q_indep_p'], data_pd_df.dtypes
    # return data_array

