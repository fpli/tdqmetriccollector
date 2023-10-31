package com.ebay.adi.adlc.tdq;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.impl.BasePipeline;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SparkApp {
    public static void main(String[] args) throws ParseException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("master");
        sparkConf.setAppName("TDQ Metric Collector");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        SparkSessionStore.getInstance().storeSparkSession(spark);

        // ignore below
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(integer -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        }).reduce((integer, integer2) -> integer + integer2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        Dataset<Row> dataset = spark.sql("select name from user");
        dataset.foreach(row -> {
            String string = row.getString(0);
        });
        // ignore above

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
