package com.ebay.adi.adlc.tdq.util;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.impl.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class PipelineFactory {

    private static volatile PipelineFactory instance;

    public static PipelineFactory getInstance() {
        if (null == instance) {
            synchronized (PipelineFactory.class) {
                if (null == instance) {
                    instance = new PipelineFactory();
                    instance.init();
                }
            }
        }
        return instance;
    }

    private PipelineFactory() {
        this.pipelineMap = new HashMap<>();
        pipelineMap.put("pageProfiling", new PageProfilingPipeline());
        pipelineMap.put("metricCollector", new MetricCollectorPipeline());
        pipelineMap.put("tagProfiling", new TagProfilingPipeline());
        pipelineMap.put("backFillRealtimeMetric", new BackFillRealtimeMetricPipeline());
    }

    private Map<String, BasePipeline<? extends BaseOption>> pipelineMap;

    public BasePipeline<? extends BaseOption> findPipeline(String bizId) {
        return pipelineMap.get(bizId);
    }

    public void init() {
        try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("application.yaml")) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            Map<String, Object> properties = objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            flatMap(properties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void flatMap(Map<String, Object> map){
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map){
                recurseReplace(map, entry.getKey(), (Map<String, Object>) entry.getValue());
            }
        }
    }

    private void recurseReplace(Map<String, Object> dest, String key, Map<String, Object> src){
        for (Map.Entry<String, Object> entry : src.entrySet()) {
            String k = key + "." + entry.getKey();
            if (entry.getValue() instanceof Map){
                recurseReplace(dest, k, (Map<String, Object>) entry.getValue());
            } else {
                dest.put(k, entry.getValue());
            }
        }
    }

}
