package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.service.BackFillRealtimeMetricOption;
import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class BackFillRealtimeMetricPipeline extends BasePipeline<BackFillRealtimeMetricOption> {

    @Override
    public BackFillRealtimeMetricOption parseCommand(String[] args) {
        BackFillRealtimeMetricOption backFillRealtimeMetricOption = new BackFillRealtimeMetricOption();
        Parser defaultParser = getDefaultParser();
        Options options = new Options();
        options.addOption("d", "date", true, "the date will be filled");
        // ...
        try {
            CommandLine commandLine = defaultParser.parse(options, args);
            String date = commandLine.getOptionValue("d");
            backFillRealtimeMetricOption.setDate(date);
            // ...
        } catch (ParseException e) {
            logger.error("parsing command line arguments {} occurred some errors:", args, e);
            throw new RuntimeException(e);
        }
        return backFillRealtimeMetricOption;
    }

    @Override
    public void process(BaseOption parameter) {
        BackFillRealtimeMetricOption backFillRealtimeMetricOption = (BackFillRealtimeMetricOption) parameter;
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
        String date = backFillRealtimeMetricOption.getDate();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime localDateTime = LocalDateTime.parse(date, dateTimeFormatter);
        LocalDate localDate = localDateTime.toLocalDate();
        String dt = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(localDate);
        //String sql = "SELECT hr, pageId, count(1) as cnt FROM ubi_w.stg_ubi_event_dump_w WHERE dt = '%s' AND type = 'nonbot' AND hr BETWEEN %d and %d GROUP BY hr, pageId ORDER BY hr ASC";
        //String actualSql = String.format(sql, dt, hour, hour1);
        //logger.info(actualSql);
        //Dataset<Row> dataset = spark.sql(actualSql);
        //List<Row> rows = dataset.collectAsList();
        String sql = " SELECT\n" +
                "  t1.hr,\n" +
                "  t1.pageid,\n" +
                "  t1.cnt,\n" +
                "  nvl(t2.late_cnt, 0) late_event_cnt\n" +
                "FROM\n" +
                "  (\n" +
                "   SELECT\n" +
                "\t  hr,\n" +
                "\t  PAGE_ID pageid,\n" +
                "\t  count(PAGE_ID) cnt\n" +
                "\tFROM\n" +
                "\t  (\n" +
                "\t\tSELECT\n" +
                "\t\t  PAGE_ID,\n" +
                "\t\t  hour(EVENT_TIMESTAMP) hr\n" +
                "\t\tFROM\n" +
                "\t\t  UBI_V.UBI_EVENT\n" +
                "\t\tWHERE\n" +
                "\t\t  SESSION_START_DT = '%s'\n" +
                "\t  ) t\n" +
                "\t  GROUP BY hr, PAGE_ID\n" +
                "  ) t1\n" +
                "  LEFT JOIN (\n" +
                "    SELECT\n" +
                "      hr,\n" +
                "      PAGEID,\n" +
                "      count(PAGEID) late_cnt\n" +
                "    FROM\n" +
                "      ubi_w.tdq_rt_late_sojevent\n" +
                "    WHERE\n" +
                "      DT = '%s'\n" +
                "      GROUP BY hr, PAGEID\n" +
                "  ) t2 ON t1.pageId = t2.PAGEID\n" +
                "  AND t1.hr = t2.hr";
        String actualSql = String.format(sql, dt, dt);
        logger.info(actualSql);
        Dataset<Row> dataset = spark.sql(actualSql);
        List<Row> rows = dataset.collectAsList();
        try {
            RestHighLevelClient restHighLevelClient = PipelineFactory.getInstance().getRestHighLevelClient();
            String index = "prod.metric.rt.page";
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
            LongAdder errorCounter = new LongAdder();
            LongAdder successCount = new LongAdder();
            rows.stream().parallel().forEach(row -> {
                try {
                    int hr = row.getInt(0);
                    int page_id = row.getInt(1);
                    long event_cnt = row.getLong(2);
                    long late_event_cnt = row.getLong(3);

                    SearchRequest searchRequest = new SearchRequest();
                    searchRequest.indices(index);
                    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                    boolQueryBuilder.must(QueryBuilders.termQuery("dt", dt));
                    boolQueryBuilder.must(QueryBuilders.termQuery("hr", hr));
                    boolQueryBuilder.must(QueryBuilders.termQuery("page_id", page_id));
                    searchSourceBuilder.query(boolQueryBuilder);
                    searchRequest.source(searchSourceBuilder);
                    final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                    final SearchHits hits = searchResponse.getHits();
                    if (hits.getTotalHits().value > 0){
                        for (SearchHit hit : hits.getHits()) {
                            String id = hit.getId();
                            UpdateRequest updateRequest = new UpdateRequest(index, id);
                            Map<String, Object> parameters = new HashMap<>();
                            parameters.put("late_event_cnt", late_event_cnt);
                            parameters.put("batch_event_cnt", event_cnt);

                            Script script = new Script(ScriptType.INLINE, "painless", "ctx._source.acc_rt_event_cnt = ctx._source.rt_event_cnt +  params.late_event_cnt;ctx._source.batch_event_cnt = params.batch_event_cnt", parameters);
                            updateRequest.script(script);

                            //indexRequest.source(payload, XContentType.JSON);
                            restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
                            successCount.increment();
                        }
                    }
                } catch (IOException e) {
                    logger.error("errors occurred in deleting index: ", e);
                    errorCounter.increment();
                }
            });
            if (errorCounter.longValue() > 0){
                logger.error("the error count is " + errorCounter.longValue());
            }
            logger.info("successfully update docs: {}", successCount.longValue());
        } catch (IOException e) {
            logger.error("something went wrong: ", e);
            throw new RuntimeException(e);
        }
    }
}
