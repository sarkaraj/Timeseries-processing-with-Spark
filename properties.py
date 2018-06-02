from product_class._products import cat_1, cat_2, cat_3, cat_4, cat_5, cat_6, cat_7, cat_8, cat_9, cat_10

annual_freq_cut_MAX = 'inf'
annual_freq_cut_MIN = 0.0

annual_freq_cut_1 = 48.0
annual_freq_cut_2 = 20.0
annual_freq_cut_3 = 12.0

# from run import total_execs

REPARTITION_STAGE_1 = 70
REPARTITION_STAGE_2 = 70

WRITE_MODE = "append"
# _model_bld_date_string_list = ['2017-09-03', '2017-09-10', '2017-09-17', '2017-09-24', '2017-10-01', '2017-10-08',
#                           '2017-10-15', '2017-10-22', '2017-10-29', '2017-11-05']

# _model_bld_date_string_list = ['2017-09-24', '2017-10-01', '2017-10-08',
#                           '2017-10-15', '2017-10-22', '2017-10-29', '2017-11-05']

_model_bld_date_string_list = ['2017-10-01']

weekly_dates = {'2017-09-03': True, '2017-09-10': True, '2017-09-17': True, '2017-09-24': True, '2017-10-01': True,
                '2017-10-08': True, '2017-10-15': True, '2017-10-22': True, '2018-04-01': True, '2018-04-08': True,
                '2018-04-29': True}

monthly_dates = {'2017-08-06': True, '2017-09-03': True, '2017-10-01': True, '2017-11-05': True, '2018-04-01': True,
                 '2018-05-06': True}

MODEL_BUILDING = "CONA_TS_MODEL_BUILD"
MODEL_TESTING = "CONA_TS_MODEL_TEST"

# weekly_pdt_cat_123_location_baseline = "/CONA_CSO/CCBF/model_eda/weekly_pdt_cat_123_baseline"
# monthly_pdt_cat_456_location_baseline = "/CONA_CSO/CCBF/model_eda/monthly_pdt_cat_456_baseline"


# weekly_pdt_cat_123_location = "/CONA_CSO/CCBC_Consolidated/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/CCBC_Consolidated/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/CCBC_Consolidated/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/CCBC_Consolidated/monthly_pdt_cat_8910"
# customer_data_location = "/CONA_CSO/CCBC_Consolidated/customer_data"
# comments = "Thaddeus Smith Route"

#
# weekly_pdt_cat_123_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/monthly_pdt_cat_8910"
# customer_data_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/customer_data"
# comments = "Thaddeus Smith Route"

# weekly_pdt_cat_123_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/monthly_pdt_cat_8910"
# customer_data_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route/customer_data"
# comments = "Thaddeus Smith Route"

# weekly_pdt_cat_123_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route_testing/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route_testing/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route_testing/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route_testing/monthly_pdt_cat_8910"
# customer_data_location = "/CONA_CSO/CCBC_Consolidated/Thaddeus_Smith_Route_testing/customer_data"
# comments = "Thaddeus Smith Route"

container = "csoproduction"
storage_account = "conapocv2standardsa.blob.core.windows.net"
PREFIX = "wasb://" + "@".join([container, storage_account])

weekly_pdt_cat_123_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/weekly_pdt_cat_123"
weekly_pdt_cat_7_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/weekly_pdt_cat_7"
monthly_pdt_cat_456_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/monthly_pdt_cat_456"
monthly_pdt_cat_8910_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/monthly_pdt_cat_8910"
customer_data_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/customer_data"
comments = ""


# weekly_pdt_cat_123_location = "/CONA_CSO/CCBF/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/CCBF/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/CCBF/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/CCBF/monthly_pdt_cat_8910"
# customer_data_location= "/CONA_CSO/CCBF/customer_data"


# weekly_pdt_cat_123_location = "/CONA_CSO/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/monthly_pdt_cat_8910"
# customer_data_location="/CONA_CSO/customer_data"

# weekly_pdt_cat_123_location = "/CONA_CSO/model_eda/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/model_eda/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/model_eda/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/model_eda/monthly_pdt_cat_8910"
# customer_data_location="/CONA_CSO/model_eda/customer_data"



if __name__ == "__main__":
    from data_fetch.support_func import generate_weekly_query

    print (generate_weekly_query(_model_bld_date_string_list))
