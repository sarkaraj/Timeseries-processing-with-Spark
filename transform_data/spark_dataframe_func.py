from pyspark.sql.types import *


def final_select_dataset(join_dataframe, sqlContext):
    join_dataframe_return = join_dataframe \
        .map(lambda _row: column_selector(_row))

    _final_df = sqlContext.createDataFrame(join_dataframe_return, schema=_schema())

    return _final_df


def column_selector(row_object):
    # CASE I: When there is a match
    if (row_object.customernumber_prophet == row_object.customernumber_arima):
        customernumber = row_object.customernumber_prophet
        matno = row_object.mat_no_prophet
        error_arima = row_object.error_arima
        pred_arima = row_object.pred_arima
        arima_params = row_object.arima_params
        error_prophet = row_object.error_prophet
        pred_prophet = row_object.pred_prophet
        prophet_params = row_object.prophet_params
        pdt_cat = row_object.pdt_cat_prophet

        result = customernumber, matno, error_arima, pred_arima, arima_params, error_prophet, pred_prophet, prophet_params, pdt_cat
        return result

    elif row_object.customernumber_prophet != row_object.customernumber_arima:
        # CASE 2 : When Prophet_model is not present
        if row_object.customernumber_prophet == None:
            customernumber = row_object.customernumber_arima
            matno = row_object.mat_no_arima
            error_arima = row_object.error_arima
            pred_arima = row_object.pred_arima
            arima_params = row_object.arima_params
            error_prophet = row_object.error_prophet
            pred_prophet = row_object.pred_prophet
            prophet_params = row_object.prophet_params
            pdt_cat = row_object.pdt_cat_arima

            result = customernumber, matno, error_arima, pred_arima, arima_params, error_prophet, pred_prophet, prophet_params, pdt_cat
            return result

        # CASE 3 : When arima_model is not present
        elif row_object.customernumber_arima == None:
            customernumber = row_object.customernumber_prophet
            matno = row_object.mat_no_prophet
            error_arima = row_object.error_arima
            pred_arima = row_object.pred_arima
            arima_params = row_object.arima_params
            error_prophet = row_object.error_prophet
            pred_prophet = row_object.pred_prophet
            prophet_params = row_object.prophet_params
            pdt_cat = row_object.pdt_cat_prophet

            result = customernumber, matno, error_arima, pred_arima, arima_params, error_prophet, pred_prophet, prophet_params, pdt_cat
            return result


def _schema():
    customernumber = StructField("customernumber", StringType(), nullable=False)
    mat_no = StructField("mat_no", StringType(), nullable=False)

    _error_arima = StructField("error_arima", MapType(StringType(), FloatType()), nullable=True)
    week_num_year = StructType([StructField("week_num", IntegerType()), StructField("week_num", IntegerType())])
    _pred_arima = StructField("pred_arima", MapType(week_num_year, FloatType()), nullable=True)
    _opt_param_arima = StructField("arima_params", MapType(StringType(), ArrayType(IntegerType())), nullable=True)

    _error_prophet = StructField("error_prophet", MapType(StringType(), FloatType()), nullable=True)
    _pred_prophet = StructField("pred_prophet", MapType(week_num_year, FloatType()), nullable=True)
    _opt_param_prophet = StructField("prophet_params", MapType(StringType(), StringType()), nullable=True)

    _pdt_category = StructField("pdt_cat", MapType(StringType(), StringType()), nullable=False)

    schema = StructType(
        [customernumber, mat_no, _error_arima, _pred_arima, _opt_param_arima, _error_prophet, _pred_prophet,
         _opt_param_prophet, _pdt_category])

    return schema
