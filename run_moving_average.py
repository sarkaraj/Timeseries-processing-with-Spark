from model.moving_average_weekly import moving_average_model_weekly
from model.moving_average_monthly import moving_average_model_monthly

def _run_moving_average(test_data, sqlContext):
    test_data_input = test_data \
        .filter(lambda x: x[1].category in ['VII', 'VIII'])

    # test_data_parallel = test_data_input.flatMap(lambda x: generate_models_prophet_monthly(x))

    ma_weekly_results_rdd = test_data_input.filter(lambda x: x[1].category == 'VII')\
        .map(lambda x: moving_average_model_weekly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[4].get_product_prop()))\
        .filter(lambda x: x != "MODEL_NOT_VALID")

    ma_monthly_results_rdd = test_data_input.filter(lambda x: x[1].category == 'VIII') \
        .map(
        lambda x: moving_average_model_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], pdt_cat=x[4].get_product_prop())) \
        .filter(lambda x: x != "MODEL_NOT_VALID")

    # opt_prophet_results_rdd = ma_weekly_results_rdd.combineByKey(dist_grid_search_create_combiner,
    #                                                            dist_grid_search_merge_value,
    #                                                            dist_grid_search_merge_combiner)

    opt_ma_weekly_results_mapped = ma_weekly_results_rdd.map(lambda line: map_for_output_prophet(line))
    opt_ma_monthly_results_mapped = ma_monthly_results_rdd.map(lambda line: map_for_output_prophet(line))

    opt_ma_weekly_results_df = sqlContext.createDataFrame(opt_ma_weekly_results_mapped, schema=prophet_output_schema())
    opt_ma_monthly_results_df = sqlContext.createDataFrame(opt_ma_monthly_results_mapped, schema=prophet_output_schema())

    # return opt_prophet_results_df, opt_prophet_results_df.count()
    return opt_ma_weekly_results_df, opt_ma_monthly_results_df