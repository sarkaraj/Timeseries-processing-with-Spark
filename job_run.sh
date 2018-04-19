#!/bin/bash

#for date_string in '2017-08-06' '2017-08-13' '2017-08-20' '2017-08-27' '2017-09-03' '2017-09-10' '2017-09-17' '2017-09-24' '2017-10-01' '2017-10-08' '2017-10-15' '2017-10-22' '2017-10-29' '2017-11-05' '2017-11-12' '2017-11-19' '2017-11-26' '2017-12-03' '2017-12-10' '2017-12-17' '2017-12-24' '2017-12-31'
#for date_string in '2017-07-23' '2017-07-30' '2017-08-06' '2017-08-13' '2017-08-20' '2017-08-27'

for date_string in '2018-04-01'
{
echo $date_string
spark-submit \
--verbose \
--master yarn \
--deploy-mode client \
--supervise \
--num-executors 30 \
--driver-memory 3G \
--executor-memory 1G \
--executor-cores 1 \
--py-files /home/sshuser/rajarshi/forecaster.zip \
/home/sshuser/rajarshi/run.py \
$date_string
}
exit 0