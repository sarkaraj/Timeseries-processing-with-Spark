def twitter_outlier_detection(data):
    import time
    from dateutil import parser
    import datetime as dt
    from pyculiarity import detect_ts
    import pandas as pd

    data = data.rename(columns={'ds': 'timestamp', 'y': 'value'})
    data.timestamp = data.timestamp.apply(str)

    data['timestamp'] = data['timestamp'].apply(lambda x:
                                                time.mktime(dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timetuple()))

    results = detect_ts(data, max_anoms=0.05, alpha=0.005, direction='both')

    # # format the twitter data nicely
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    data.loc[(data.timestamp.isin(results['anoms'].index)), 'value'] = None
    data = data.rename(columns={'timestamp': 'ds', 'value': 'y'})
    data.ds = data.ds.dt.date
    data.ds = data.ds.apply(str).apply(parser.parse)
    data.y = data.y.interpolate(method='cubic', limit=10, limit_direction='both')

    return (data)


def ensm_mod(prod, cus_no, mat_no, holidays, min_train_days=731, test_points=2):
    import pandas as pd
    import numpy as np
    # import pyflux as pf
    from pyculiarity import detect_ts
    import itertools
    import warnings
    import statsmodels.api as sm
    from fbprophet import Prophet
    from dateutil import parser
    import datetime as dt

    prod = prod.rename(columns={'dt_week': 'ds', 'quantity': 'y'})
    prod = prod[['ds', 'y']]
    prod.ds = prod.ds.apply(str).apply(parser.parse)
    prod.y = prod.y.apply(float)
    prod = prod.sort_values('ds')
    prod = prod.reset_index(drop=True)
    prod = prod.drop(prod.index[[0, len(prod.y) - 1]]).reset_index(drop=True)

    prod = twitter_outlier_detection(prod)

    train = prod[
        prod.ds <= (np.amax(prod.ds) - pd.DateOffset(days=(np.amax(prod.ds) - np.amin(prod.ds)).days - min_train_days))]
    test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
    rem_data = prod[(np.amax(np.array(train.index)) + test_points):]
    output_result = pd.DataFrame()

    while (len(rem_data.ds) >= test_points):

        # ARIMA Model Data Transform
        train_arima = train.set_index('ds', drop=True)
        # train_arima.head()
        test_arima = test.set_index('ds', drop=True)

        # ARIMA Model
        # grid search parameters
        p = [0, 1]
        d = [0, 1]
        q = [0, 1]

        # Generate all different combinations of p, q and q triplets
        pdq = list(itertools.product(p, d, q))

        # Generate all different combinations of seasonal p, q and q triplets
        seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(p, d, q))]

        print('Next Test Starts...')

        # grid search
        warnings.filterwarnings("ignore")  # specify to ignore warning messages
        min_aic = 9999999
        for param in pdq:
            for param_seasonal in seasonal_pdq:
                try:
                    mod = sm.tsa.statespace.SARIMAX(train_arima, order=param, seasonal_order=param_seasonal,
                                                    enforce_stationarity=False, enforce_invertibility=False,
                                                    measurement_error=True)

                    results = mod.fit()
                    if results.aic < min_aic:
                        min_aic = results.aic
                        opt_param = param
                        opt_param_seasonal = param_seasonal

                    #                     print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
                except:
                    continue
                #         print('Optimal ARIMA{}x{}12 - AIC:{}'.format(opt_param, opt_param_seasonal, min_aic))

        # fitting Model
        mod = sm.tsa.statespace.SARIMAX(train_arima, order=opt_param, seasonal_order=opt_param_seasonal,
                                        enforce_stationarity=False, enforce_invertibility=False, measurement_error=True)
        result = mod.fit(disp=False)

        # forecast Train
        pred_train = results.get_prediction(start=pd.to_datetime(np.amin(np.array(train_arima.index))), dynamic=False)
        pred_train_ci = pred_train.conf_int()

        # forecast test
        pred_test = results.get_prediction(start=pd.to_datetime(np.amax(train_arima.index)),
                                           end=pd.to_datetime(np.amax(test_arima.index)), dynamic=True)
        pred_test_ci = pred_test.conf_int()

        # ceating test and train emsembled result
        # test result
        result_test = test
        result_test['y_ARIMA'] = np.array(pred_test.predicted_mean)[1:]

        # prophet
        m = Prophet(weekly_seasonality=False, holidays=holidays, yearly_seasonality=True, changepoint_prior_scale=5)
        m.fit(train);

        # creating pred train and test data frame
        past = m.make_future_dataframe(periods=0, freq='W')
        future = pd.DataFrame(test['ds'])
        pf_train_pred = m.predict(past)
        pf_test_pred = m.predict(future)
        pf_train_pred = pf_train_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([past.index])
        pf_test_pred = pf_test_pred[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].set_index([future.index])

        # ceating test and train emsembled result
        # test result
        result_test['y_Prophet'] = np.array(pf_test_pred.yhat)

        # Ansemble
        result_test['y_Ensembled'] = result_test[["y_ARIMA", "y_Prophet"]].mean(axis=1)

        train = prod[:(np.amax(np.array(train.index)) + 1 + test_points)]
        test = prod[(np.amax(np.array(train.index)) + 1):(np.amax(np.array(train.index)) + 1 + test_points)]
        rem_data = prod[(np.amax(np.array(train.index)) + test_points):]

        output_result = pd.concat([output_result, result_test], axis=0)

    output_result['Error'] = np.subtract(output_result.y_Ensembled, output_result.y)
    output_result['Error_Cumsum'] = output_result.Error.cumsum() / output_result.y.cumsum() * 100

    output_result['Error_prophet'] = np.subtract(output_result.y_Prophet, output_result.y)
    output_result['Error_Cumsum_prophet'] = output_result.Error_prophet.cumsum() / output_result.y.cumsum() * 100

    output_result['Error_arima'] = np.subtract(output_result.y_ARIMA, output_result.y)
    output_result['Error_Cumsum_arima'] = output_result.Error_arima.cumsum() / output_result.y.cumsum() * 100

    output_result['rolling_6week_error'] = pd.rolling_sum(output_result['Error'], window=6, min_periods=6, freq=None,
                                                          center=False, how=None, )
    output_result['rolling_6week_y'] = pd.rolling_sum(output_result['y'], window=6, min_periods=6, freq=None,
                                                      center=False, how=None, )
    output_result['rolling_6week_percent_error'] = output_result['rolling_6week_error'] / output_result[
        'rolling_6week_y'] * 100

    output_result['rolling_12week_error'] = pd.rolling_sum(output_result['Error'], window=12, min_periods=12, freq=None,
                                                           center=False, how=None, )
    output_result['rolling_12week_y'] = pd.rolling_sum(output_result['y'], window=12, min_periods=12, freq=None,
                                                       center=False, how=None, )
    output_result['rolling_12week_percent_error'] = output_result['rolling_12week_error'] / output_result[
        'rolling_12week_y'] * 100

    output_result['rolling_6week_error_arima'] = pd.rolling_sum(output_result['Error_arima'], window=6, min_periods=6,
                                                                freq=None, center=False, how=None, )
    output_result['rolling_6week_y'] = pd.rolling_sum(output_result['y'], window=6, min_periods=6, freq=None,
                                                      center=False, how=None, )
    output_result['rolling_6week_percent_error_arima'] = output_result['rolling_6week_error_arima'] / output_result[
        'rolling_6week_y'] * 100

    output_result['rolling_12week_error_arima'] = pd.rolling_sum(output_result['Error_arima'], window=12,
                                                                 min_periods=12, freq=None, center=False, how=None, )
    output_result['rolling_12week_y'] = pd.rolling_sum(output_result['y'], window=12, min_periods=12, freq=None,
                                                       center=False, how=None, )
    output_result['rolling_12week_percent_error_arima'] = output_result['rolling_12week_error_arima'] / output_result[
        'rolling_12week_y'] * 100

    output_result['rolling_6week_error_prophet'] = pd.rolling_sum(output_result['Error_prophet'], window=6,
                                                                  min_periods=6, freq=None, center=False, how=None, )
    output_result['rolling_6week_y'] = pd.rolling_sum(output_result['y'], window=6, min_periods=6, freq=None,
                                                      center=False, how=None, )
    output_result['rolling_6week_percent_error_prophet'] = output_result['rolling_6week_error_prophet'] / output_result[
        'rolling_6week_y'] * 100

    output_result['rolling_12week_error_prophet'] = pd.rolling_sum(output_result['Error_prophet'], window=12,
                                                                   min_periods=12, freq=None, center=False, how=None, )
    output_result['rolling_12week_y'] = pd.rolling_sum(output_result['y'], window=12, min_periods=12, freq=None,
                                                       center=False, how=None, )
    output_result['rolling_12week_percent_error_prophet'] = output_result['rolling_12week_error_prophet'] / \
                                                            output_result['rolling_12week_y'] * 100

    output = pd.DataFrame(data=[[cus_no, mat_no, np.nanmedian(output_result.rolling_6week_percent_error),
                                 np.nanmedian(output_result.rolling_6week_percent_error_prophet),
                                 np.nanmedian(output_result.rolling_6week_percent_error_arima),
                                 np.nanmedian(output_result.rolling_12week_percent_error),
                                 np.nanmedian(output_result.rolling_12week_percent_error_prophet),
                                 np.nanmedian(output_result.rolling_12week_percent_error_arima),
                                 np.nanmedian(output_result.Error_Cumsum)]],
                          columns=['cus_no', 'mat_no', '6week_rolling_error', '6week_rolling_error_prophet',
                                   '6week_rolling_error_arima',
                                   '12week_rolling_error', '12week_rolling_error_prophet',
                                   '12week_rolling_error_arima', 'cumulative_error'])

    return output