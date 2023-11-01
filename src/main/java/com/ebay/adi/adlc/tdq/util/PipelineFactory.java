package com.ebay.adi.adlc.tdq.util;

import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.impl.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    public RestHighLevelClient getRestHighLevelClient() {
        String hostname = configMap.get("pronto.hostname").toString();
        String schema   = configMap.get("pronto.scheme").toString();
        String username = configMap.get("pronto.api-key").toString();
        String password = configMap.get("api-value").toString();
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, 9200, schema));
        BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
        basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        restClientBuilder.setHttpClientConfigCallback( httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider));
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(1000 * 5).setSocketTimeout(1000 * 60));
        return new RestHighLevelClient(restClientBuilder);
    }

    public Connection getMySQLConnection() throws Exception {
        String url = configMap.get("mysql.jdbc.url").toString();
        String driver = configMap.get("mysql.jdbc.driver-class-name").toString();
        String user = configMap.get("mysql.jdbc.u").toString();
        String password = configMap.get("mysql.jdbc.p").toString();
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }

    private ConcurrentMap<String, Object> configMap;

    public void init() {
        try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("application.yaml")) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            Map<String, Object> properties = objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            configMap = new ConcurrentHashMap<>(properties);
            flatMap(configMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void flatMap(ConcurrentMap<String, Object> map){
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map){
                recurseReplace(map, entry.getKey(), (Map<String, Object>) entry.getValue());
                map.remove(entry.getKey());
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
