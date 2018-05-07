def _run_dist_arima_monthly(test_data, sqlContext, **kwargs):
    # LIBRARY IMPORTS
    from distributed_grid_search._model_params_set import generate_models_sarimax_monthly
    from transform_data.rdd_to_df import map_for_output_arima, arima_output_schema

    from support_func import dist_grid_search_create_combiner, dist_grid_search_merge_value, \
        dist_grid_search_merge_combiner

    from distributed_grid_search._sarimax_monthly import sarimax_monthly
    from properties import REPARTITION_STAGE_1, REPARTITION_STAGE_2

    # ###################

    test_data_input = test_data \
        .filter(lambda x: x[1].category in ('IV', 'V', 'VI'))

    MODEL_BLD_CURRENT_DATE = kwargs.get('MODEL_BLD_CURRENT_DATE')

    test_data_parallel = test_data_input.flatMap(
        lambda x: generate_models_sarimax_monthly(x,
                                                  MODEL_BLD_CURRENT_DATE=MODEL_BLD_CURRENT_DATE))  # # gets 587 * 55 = 32285 rows

    # # Parallelizing Jobs
    arima_results_rdd = test_data_parallel \
        .repartition(REPARTITION_STAGE_1) \
        .map(lambda x: sarimax_monthly(cus_no=x[0], mat_no=x[1], prod=x[2], pdq=x[2], seasonal_pdq=x[3],
                                           min_train_days=x[4].min_train_days, pdt_cat=x[4].get_product_prop())) \
        .filter(lambda x: x != "MODEL_NOT_VALID")
    # .repartition(REPARTITION_STAGE_1)

    opt_arima_results_rdd = arima_results_rdd \
        .combineByKey(dist_grid_search_create_combiner,
                      dist_grid_search_merge_value,
                      dist_grid_search_merge_combiner,
                      numPartitions=REPARTITION_STAGE_2)

    opt_arima_results_mapped = opt_arima_results_rdd.map(lambda line: map_for_output_arima(line))

    opt_arima_results_df = sqlContext.createDataFrame(opt_arima_results_mapped, schema=arima_output_schema())

    return opt_arima_results_df