#!/bin/bash

while getopts ":d:l:" opt
do
  case $opt in
    d)
      model_bld_date=$OPTARG
      ;;
    l)
      on_demand_sales_loc=$OPTARG
      ;;
    :)
      echo "Option -$OPTARG requires an argument."
      exit 1
      ;;
  esac
done


spark-submit \
--verbose \
--master yarn \
--deploy-mode client \
--num-executors 35 \
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
~/cso_predictor_prod/run_on_demand.py \
$model_bld_date \
$on_demand_sales_loc

# '2018-06-03'
# --queue tsmdl \

exit 0
