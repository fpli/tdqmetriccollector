package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.TagProfilingOption;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.spark.sql.SparkSession;

public class TagProfilingPipeline extends BasePipeline<TagProfilingOption> {

    @Override
    public TagProfilingOption parseCommand(String[] args) {
        // todo parse command args to build java pojo
        return new TagProfilingOption();
    }

    @Override
    public void process(TagProfilingOption parameter) {
//        super.process(parameter);
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
    }
}
