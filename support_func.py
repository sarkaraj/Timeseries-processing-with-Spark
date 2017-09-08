from model.weekly_model import weekly_ensm_model
from transform_data.data_transform import get_weekly_aggregate
from transform_data.pandas_support_func import *


def model_fit(row_object, holiday_list):
    # import sys
    # sys.path.append('/home/SSHAdmin/.local/lib/python2.7/site-packages/')
    # sys.path.append('/home/SSHAdmin/anaconda/lib/python2.7/site-packages/')

    import pandas as pd

    customernumber = row_object.customernumber
    matnr = row_object.matnr
    # pdt_freq_annual = row_object.pdt_freq_annual

    holidays = convert_list_to_pd_df(holiday_list)

    # Unpacking the dataset
    data_array = [row.split("\t") for row in row_object.data]
    data_pd_df = pd.DataFrame(data_array, columns=['date', 'quantity', 'q_indep_p'])
    data_pd_df['customernumber'] = customernumber
    data_pd_df['matnr'] = matnr
    data_pd_df['quantity'] = data_pd_df['quantity'].map(float)
    data_pd_df['q_indep_p'] = data_pd_df['q_indep_p'].map(float)

    # data_pd_df = data_pd_df.sort_values('date', ascending=True)

    data_pd_df = get_weekly_aggregate(data_pd_df)
    # ensm_mod(prod, cus_no, mat_no, holidays, min_train_days=731, test_points=2)
    output = weekly_ensm_model(prod=data_pd_df, cus_no=customernumber, mat_no=matnr, holidays=holidays)

    # # Segregating Dataset based in annual product frequency
    # if(pdt_freq_annual >= 12 and pdt_freq_annual < 52):
    #     return pd.Dataframe()
    #     # data_pd_df = get_monthly_aggregate(data_pd_df)
    #     # model = some_func_monthly(data_pd_df)
    #     # return (customernumber, matnr, model, <array of images>)
    #     # pass
    # elif(pdt_freq_annual > 52):
    #     data_pd_df = get_weekly_aggregate(data_pd_df)
    #     # ensm_mod prod,cus_no,mat_no, holidays
    #     return ensm_mod(prod=data_pd_df, cus_no=customernumber, mat_no=matnr, holidays=holidays)
    #     # pass

    # else:
    #     # data_pd_df = pd.DataFrame()
    #     return pd.Dataframe()
    #     # pass
    # # Changed output to a dicionary of structure {index -> {column -> value}}
    # return (customernumber, matnr, output)

    # output_list = extract_from_dict(output.to_dict(orient='index'))

    return output

