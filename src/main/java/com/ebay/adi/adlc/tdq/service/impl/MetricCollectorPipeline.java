package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.MetricCollectorOption;

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
    }
}
