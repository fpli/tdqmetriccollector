package com.ebay.adi.adlc.tdq;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.impl.BasePipeline;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkApp {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("master");
        sparkConf.setAppName("TDQ Metric Collector");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        SparkSessionStore.getInstance().storeSparkSession(spark);

        DefaultParser defaultParser = new DefaultParser();
        Options options = new Options();
        Option option = new Option("bizId", "bizId", true, "the identify of this business");
        options.addOption(option);
        CommandLine commandLine = defaultParser.parse(options, args);
        String bizId = commandLine.getOptionValue(option);

        BasePipeline<? extends BaseOption> pipeline = PipelineFactory.getInstance().findPipeline(bizId);

        BaseOption baseOption = pipeline.parseCommand(args);

        pipeline.process(baseOption);

        spark.stop();
    }
}
