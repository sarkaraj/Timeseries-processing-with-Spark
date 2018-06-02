#!/bin/bash

model_bld_date=$(date +'%Y-%m-%d')

# echo $model_bld_date

spark-submit \
--verbose \
--master yarn \
--deploy-mode client \
--queue tsmdl \
--num-executors 20 \
--driver-memory 3G \
--executor-memory 5G \
--executor-cores 2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.speculation=true \
--conf spark.speculation.multiplier=1.5 \
--conf spark.speculation.quantile=0.9 \
--conf spark.speculation.interval=18000 \
--conf spark.sql.shuffle.partitions=70 \
--py-files ~/cso_predictor_prod/forecaster.zip \
~/cso_predictor_prod/run.py \
'2018-05-27'

# $model_bld_date

exit 0



# spark-submit \
# --master yarn \
# --deploy-mode client \
# --supervise \
# --queue tsmdl \
# --driver-memory 10G \
# --executor-memory 4G \
# --num-executors 30 \
# --executor-cores 1 \
# --conf spark.dynamicAllocation.enabled=false \
# --conf spark.speculation=true \
# --conf spark.speculation.multiplier=3 \
# --conf spark.speculation.quantile=0.9 \
# --conf spark.speculation.interval=900000 \
# --py-files ~/cso_predictor/forecaster.zip \
# ~/cso_predictor/_monthly_products.py \
# $date_string