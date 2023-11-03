package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.BackFillRealtimeMetricOption;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.commons.cli.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;

public class BackFillRealtimeMetricPipeline extends BasePipeline<BackFillRealtimeMetricOption> {

    @Override
    public BackFillRealtimeMetricOption parseCommand(String[] args) {
        BackFillRealtimeMetricOption backFillRealtimeMetricOption = new BackFillRealtimeMetricOption();
        DefaultParser defaultParser = getDefaultParser();
        Options options = new Options();
        Option startOpt = new Option("start", "start", true, "the start time of the range");
        Option endOpt = new Option("end", "end", true, "the end time of the range");
        // ...
        try {
            CommandLine commandLine = defaultParser.parse(options, args);
            String start = commandLine.getOptionValue(startOpt);
            backFillRealtimeMetricOption.setStart(start);
            String end = commandLine.getOptionValue(endOpt);
            backFillRealtimeMetricOption.setEnd(end);
            // ...
        } catch (ParseException e) {
            logger.error("parsing command line arguments {} occurred some errors:", args, e);
            throw new RuntimeException(e);
        }
        return backFillRealtimeMetricOption;
    }

    @Override
    public void process(BackFillRealtimeMetricOption backFillRealtimeMetricOption) {
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
        Dataset<Row> dataset = spark.sql("");

        RestHighLevelClient restHighLevelClient = PipelineFactory.getInstance().getRestHighLevelClient();
    }
}
