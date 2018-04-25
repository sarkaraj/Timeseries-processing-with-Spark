#!/bin/bash

#for date_string in '2017-08-06' '2017-08-13' '2017-08-20' '2017-08-27' '2017-09-03' '2017-09-10' '2017-09-17' '2017-09-24' '2017-10-01' '2017-10-08' '2017-10-15' '2017-10-22' '2017-10-29' '2017-11-05' '2017-11-12' '2017-11-19' '2017-11-26' '2017-12-03' '2017-12-10' '2017-12-17' '2017-12-24' '2017-12-31'
#for date_string in '2017-07-23' '2017-07-30' '2017-08-06' '2017-08-13' '2017-08-20' '2017-08-27'
#  '2018-04-08'

for date_string in '2018-04-01'
{
echo $date_string
spark-submit \
--master yarn \
--deploy-mode client \
--supervise \
--queue tsmdl \
--driver-memory 10G \
--executor-memory 3G \
--num-executors 30 \
--executor-cores 1 \
--py-files ~/cso_predictor/forecaster.zip \
~/cso_predictor/_monthly_products.py \
$date_string
}
exit 0




# spark-submit \
# --verbose \
# --master yarn \
# --deploy-mode client \
# --supervise \
# --queue tsmdl \
# --num-executors 30 \
# --driver-memory 5G \
# --executor-memory 2G \
# --executor-cores 1 \
# --py-files ~/cso_predictor/forecaster.zip \
# ~/cso_predictor/_monthly_products.py \
# $date_string