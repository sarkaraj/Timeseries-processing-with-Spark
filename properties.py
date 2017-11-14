from product_class._products import cat_1, cat_2, cat_3, cat_4, cat_5, cat_6, cat_7, cat_8, cat_9, cat_10

# from data_fetch.properties import MODEL_BLD_CURRENT_DATE

_model_bld_date_string = ['2017-09-03', '2017-09-10', '2017-09-17', '2017-09-24', '2017-10-01', '2017-10-08',
                          '2017-10-15', '2017-10-22', '2017-10-29', '2017-11-05']
# _model_bld_date_string = ['2017-09-03', '2017-10-01']

# weekly_dates = {'2017-09-03': True, '2017-09-10': True, '2017-09-17': True,'2017-09-24': True, '2017-10-01': True, '2017-10-08': True, '2017-10-15': True, '2017-10-22': True}

monthly_dates = {'2017-09-03': True, '2017-10-01': True, '2017-11-05': True}

MODEL_BUILDING = "CONA_TS_MODEL_BUILD"
MODEL_TESTING = "CONA_TS_MODEL_TEST"

# weekly_pdt_cat_123_location = "/CONA_CSO/weekly_pdt_cat_123"
# weekly_pdt_cat_7_location = "/CONA_CSO/weekly_pdt_cat_7"
# monthly_pdt_cat_456_location = "/CONA_CSO/monthly_pdt_cat_456"
# monthly_pdt_cat_8910_location = "/CONA_CSO/monthly_pdt_cat_8910"

weekly_pdt_cat_123_location = "/CONA_CSO/model_eda/weekly_pdt_cat_123"
weekly_pdt_cat_7_location = "/CONA_CSO/model_eda/weekly_pdt_cat_7"
monthly_pdt_cat_456_location = "/CONA_CSO/model_eda/monthly_pdt_cat_456"
monthly_pdt_cat_8910_location = "/CONA_CSO/model_eda/monthly_pdt_cat_8910"


if __name__ == "__main__":
    from data_fetch.support_func import generate_weekly_query

    print generate_weekly_query(_model_bld_date_string)
