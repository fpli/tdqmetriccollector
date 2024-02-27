#!/bin/bash
# This is for data profiling batch
SPARK_HOME=/apache/spark
begin_time=$(date +%s)
printf "date: %s \n" $1
${SPARK_HOME}/bin/spark-submit --queue hdlq-data-batch-ubi-sle --conf spark.binary.majorVersion=3.1.1 --conf spark.hadoop.majorVersion=3 --files /apache/confs/hive/conf/hive-site.xml --master yarn --deploy-mode cluster --executor-memory 32g --class com.ebay.adi.adlc.tdq.SparkApp viewfs://apollo-rno/apps/b_adlc/o_ubi/tdqmetriccollector/1.0.0/latest/app.jar "pageMetadataQuality" --date "20240109000000"
exitCode=$?
echo "step 1 ----"
end_time=$(date +%s)
duration=$(( $end_time - $begin_time ))
printf "duration = %d\n" $duration
exit $exitCode
