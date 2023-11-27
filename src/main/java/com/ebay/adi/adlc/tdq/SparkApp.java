package com.ebay.adi.adlc.tdq;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.impl.BasePipeline;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SparkApp {

    private static Logger logger = LoggerFactory.getLogger(SparkApp.class);
    public static void main(String[] args) {
        logger.info("args: {}", Arrays.toString(args));
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("yarn");
//        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("TDQ Metric Collector");

        SparkSession spark = SparkSession
                .builder()
                .config("hive.exec.dynamic.partition", "dynamic")
                .config("hive.exec.dynamic.partition.mode", "nonstrict")
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        SparkSessionStore.getInstance().storeSparkSession(spark);

        String bizId = args[0];

        BasePipeline<? extends BaseOption> pipeline = PipelineFactory.getInstance().findPipeline(bizId);

        BaseOption baseOption = pipeline.parseCommand(args);

        pipeline.process(baseOption);

//        pipeline.process(Optional.of(baseOption));

        spark.stop();
    }
}
