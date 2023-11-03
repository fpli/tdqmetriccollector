package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.BackFillRealtimeMetricOption;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

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
        String start = backFillRealtimeMetricOption.getStart();
        String end = backFillRealtimeMetricOption.getEnd();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(start, dateTimeFormatter);
        LocalDateTime localDateTime2 = LocalDateTime.parse(end, dateTimeFormatter);
        int hour = localDateTime.getHour();
        int hour1 = localDateTime2.getHour();
        LocalDate localDate = localDateTime.toLocalDate();
        String dt = DateTimeFormatter.ofPattern("yyyyMMdd").format(localDate);
        String dtt = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(localDate);
        String sql = "SELECT hr, pageId, count(1) as cnt FROM ubi_w.stg_ubi_event_dump_w WHERE dt = '%s' AND type = 'nonbot' AND hr BETWEEN %d and %d GROUP BY hr, pageId ORDER BY hr ASC";
        String actualSql = String.format(sql, dt, hour, hour1);
        Dataset<Row> dataset = spark.sql(actualSql);
        Row[] rows = dataset.collect();
        try {
            RestHighLevelClient restHighLevelClient = PipelineFactory.getInstance().getRestHighLevelClient();
            String index = "xxx";
            GetIndexRequest getIndexRequest = new GetIndexRequest(index);
            boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!exists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                boolean acknowledged = createIndexResponse.isAcknowledged();
                if (acknowledged){
                    logger.info("index: {} has been created.", index);
                } else {
                    logger.error("index: {} creation was failed!", index);
                }
            }
            String metric_key = "hourly_event_cnt";
            String create_time = LocalDateTime.now().toString();
            long metric_time = System.nanoTime();
            LongAdder errorCounter = new LongAdder();
            ObjectMapper objectMapper = new ObjectMapper();
            Base64.Encoder encoder = Base64.getEncoder();
            Arrays.stream(rows).parallel().forEach(row -> {
                String hr = row.getString(0);
                int page_id = row.getInt(1);
                long event_cnt = row.getLong(2);
                DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
                deleteByQueryRequest.setConflicts("proceed");
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                boolQueryBuilder.must(QueryBuilders.termQuery("metric_key", metric_key));
                boolQueryBuilder.must(QueryBuilders.termQuery("dt", dt));
                boolQueryBuilder.must(QueryBuilders.termQuery("page_id", page_id));
                boolQueryBuilder.must(QueryBuilders.termQuery("hr", hr));

                deleteByQueryRequest.setQuery(boolQueryBuilder);
                try {
                    restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
                    Map<String, Object> map = new HashMap<>();
                    map.put("event_cnt", event_cnt);
                    map.put("create_time", create_time);
                    map.put("hr", hr);
                    map.put("metric_time", metric_time);
                    map.put("dt", dt);
                    map.put("metric_key", metric_key);
                    map.put("page_id", page_id);
                    IndexRequest indexRequest = new IndexRequest(index);
                    indexRequest.id(encoder.encodeToString((page_id + "." + dtt + hr).getBytes()));
                    String payload = objectMapper.writeValueAsString(map);
                    indexRequest.source(payload, XContentType.JSON);
                    restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    logger.error("errors occurred in deleting index: ", e);
                    errorCounter.increment();
                }
            });

        } catch (IOException e) {
            logger.error("something went wrong: ", e);
            throw new RuntimeException(e);
        }
    }
}
