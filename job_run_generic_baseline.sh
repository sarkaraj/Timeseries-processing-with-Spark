#!/usr/bin/env bash

#!/bin/bash

model_bld_date=$(date +'%Y-%m-%d')

# echo $model_bld_date

spark-submit \
--verbose \
--master yarn \
--deploy-mode client \
--queue tsmdl \
--num-executors 15 \
--driver-memory 5G \
--executor-memory 4G \
--executor-cores 2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.speculation=true \
--conf spark.speculation.multiplier=1.5 \
--conf spark.speculation.quantile=0.9 \
--conf spark.speculation.interval=18000 \
--conf spark.sql.shuffle.partitions=70 \
--py-files ~/cso_predictor_prod/forecaster.zip \
~/cso_predictor_prod_baseline/run_baseline.py \
'2018-07-01'

#$model_bld_date

exit 0