from product_class._products import cat_1, cat_2, cat_3, cat_4, cat_5, cat_6, cat_7, cat_8, cat_9, cat_10
import product_class.properties as p

WRITE_MODE = "overwrite"  # TODO: Change it to 'append' when merging with production branch

_model_bld_date_string_list = ['2017-10-01']

weekly_dates = {'2017-09-03': True, '2017-09-10': True, '2017-09-17': True, '2017-09-24': True, '2017-10-01': True,
                '2017-10-08': True, '2017-10-15': True, '2017-10-22': True, '2018-04-01': True, '2018-04-08': True,
                '2018-04-29': True}

monthly_dates = {'2017-08-06': True, '2017-09-03': True, '2017-10-01': True, '2017-11-05': True, '2018-04-01': True,
                 '2018-05-06': True}

###################################################################################################
# ___________________________________CONTROL PARAMETERS___________________________________________
###################################################################################################

annual_freq_cut_MAX = p.annual_freq_cut_MAX
annual_freq_cut_MIN = p.annual_freq_cut_MIN

annual_freq_cut_1 = p.annual_freq_cut_1
annual_freq_cut_2 = p.annual_freq_cut_2
annual_freq_cut_3 = p.annual_freq_cut_3

MODEL_BUILDING = "CONA_TS_MODEL_BUILD"
MODEL_TESTING = "CONA_TS_MODEL_TEST"

REPARTITION_STAGE_1 = 70
REPARTITION_STAGE_2 = 70

###################################################################################################
# ___________________________________STORAGE LOCATION______________________________________________
###################################################################################################
container = "csotestenv"
# container = "csoproduction"  # TODO: Uncomment when merging with production branch

if container == "csoproduction":
    CUSTOMER_SAMPLING = False
else:
    CUSTOMER_SAMPLING = True

storage_account = "conapocv2standardsa.blob.core.windows.net"
PREFIX = "wasb://" + "@".join([container, storage_account])

weekly_pdt_cat_123_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/weekly_pdt_cat_123"
weekly_pdt_cat_7_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/weekly_pdt_cat_7"
monthly_pdt_cat_456_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/monthly_pdt_cat_456"
monthly_pdt_cat_8910_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/monthly_pdt_cat_8910"
customer_data_location = PREFIX + "/CONA_CSO/CCBCC_Consolidated/customer_data"
test_delivery_routes = PREFIX + "/CONA_CSO/CCBCC_Consolidated/test_delivery_routes"
VISIT_LIST_LOCATION = "wasb://skuopt@conapocv2standardsa.blob.core.windows.net/AZ_TCAS_VL.csv"
comments = ""

weekly_pdt_cat_123_location_baseline = PREFIX + "/CONA_CSO/CCBCC_Consolidated/weekly_pdt_cat_123_baseline"
monthly_pdt_cat_456_location_baseline = PREFIX + "/CONA_CSO/CCBCC_Consolidated/monthly_pdt_cat_456_baseline"

CUSTOMER_SAMPLING_PERCENTAGE = 1  # has to be within 1
###################################################################################################
# _________________________________________EOF____________________________________________________
###################################################################################################


if __name__ == "__main__":
    print(cat_1.get_product_prop())
