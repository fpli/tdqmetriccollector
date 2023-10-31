package com.ebay.adi.adlc.tdq.util;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.impl.BasePipeline;
import com.ebay.adi.adlc.tdq.service.impl.MetricCollectorPipeline;
import com.ebay.adi.adlc.tdq.service.impl.PageProfilingPipeline;

import java.util.HashMap;
import java.util.Map;

public class PipelineFactory {

    private static volatile PipelineFactory instance;

    public static PipelineFactory getInstance() {
        if (null == instance) {
            synchronized (PipelineFactory.class) {
                if (null == instance) {
                    instance = new PipelineFactory();
                }
            }
        }
        return instance;
    }

    private PipelineFactory() {
        this.pipelineMap = new HashMap<>();
        pipelineMap.put("pageProfiling",   new PageProfilingPipeline());
        pipelineMap.put("metricCollector", new MetricCollectorPipeline());
    }

    private Map<String, BasePipeline<? extends BaseOption>> pipelineMap;

    public BasePipeline<? extends BaseOption> findPipeline(String bizId) {
        return pipelineMap.get(bizId);
    }

}
