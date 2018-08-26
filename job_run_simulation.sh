#!/usr/bin/env bash

while getopts ":d:" opt
do
  case $opt in
    d)
      model_bld_date=$OPTARG
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
--driver-memory 10G \
--executor-memory 5G \
--executor-cores 2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.speculation=true \
--conf spark.speculation.multiplier=1.5 \
--conf spark.speculation.quantile=0.9 \
--conf spark.speculation.interval=18000 \
--conf spark.sql.shuffle.partitions=70 \
--py-files $HOME/cso_simulator_master/cso_predictor_prod/forecaster.zip \
$HOME/cso_simulator_master/cso_predictor_prod/run_sim.py \
$model_bld_date

exit 0