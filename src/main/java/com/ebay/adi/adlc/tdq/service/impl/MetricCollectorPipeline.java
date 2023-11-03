package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.MetricCollectorOption;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestHighLevelClient;

import java.sql.Connection;

public class MetricCollectorPipeline extends BasePipeline<MetricCollectorOption> {
    @Override
    public MetricCollectorOption parseCommand(String[] args) {
        //super.parseCommand(args);
        // todo parse command args to build java pojo
        return new MetricCollectorOption();
    }

    @Override
    public void process(MetricCollectorOption parameter) {
        //super.process(parameter);
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
        try {
            Connection connection = PipelineFactory.getInstance().getMySQLConnection();
            RestHighLevelClient restHighLevelClient = PipelineFactory.getInstance().getRestHighLevelClient();

        } catch (Exception e) {
            logger.error("get errors", e);
            throw new RuntimeException(e);
        }

    }
}
