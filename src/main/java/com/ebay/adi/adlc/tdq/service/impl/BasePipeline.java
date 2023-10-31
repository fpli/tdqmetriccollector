package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.Pipeline;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.spark.sql.SparkSession;

public abstract class BasePipeline<T extends BaseOption> implements Pipeline<T> {

    @Override
    public T parseCommand(String[] args) {

        return null;
    }

    @Override
    public void process(BaseOption parameter) {
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
    }


}
