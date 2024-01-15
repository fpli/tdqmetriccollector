package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.dto.PagePoolMapping;
import com.ebay.adi.adlc.tdq.service.BaseOption;
import com.ebay.adi.adlc.tdq.service.PageMetadataOption;
import com.ebay.adi.adlc.tdq.util.PipelineFactory;
import com.ebay.adi.adlc.tdq.util.SparkSessionStore;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PageMetadataQualityPipeline extends BasePipeline<PageMetadataOption> {
    @Override
    public PageMetadataOption parseCommand(String[] args) {
        PageMetadataOption pageMetadataOption = new PageMetadataOption();
        // todo parse args to build pageMetadataOption
        Parser defaultParser = getDefaultParser();
        Options options = new Options();
        options.addOption("d", "date", true, "the date will be filled");
        try {
            CommandLine commandLine = defaultParser.parse(options, args);
            String date = commandLine.getOptionValue("d");
            pageMetadataOption.setDate(date);
        } catch (ParseException e) {
            logger.error("parsing command line arguments {} occurred some errors:", args, e);
            throw new RuntimeException(e);
        }
        return pageMetadataOption;
    }

    @Override
    public void process(BaseOption parameter) {
        PageMetadataOption pageMetadataOption = (PageMetadataOption) parameter;
        // todo based on pageMetadataOption then to process page metadata quality
        String date = pageMetadataOption.getDate();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime localDateTime = LocalDateTime.parse(date, dateTimeFormatter);
        LocalDate localDate = localDateTime.toLocalDate();
        DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyyMMdd");
        String dt = dateTimeFormatter1.format(localDate);
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
        String sparkSqlTemplate = "SELECT\n" +
                                    "  t1.page_id,\n" +
                                    "  t1.traffic,\n" +
                                    "  t2.pool_name\n" +
                                    "FROM\n" +
                                    "  (\n" +
                                    "    SELECT\n" +
                                    "      page_id,\n" +
                                    "      traffic\n" +
                                    "    FROM\n" +
                                    "      (\n" +
                                    "        SELECT\n" +
                                    "          page_id,\n" +
                                    "          sum(event_cnt) traffic\n" +
                                    "        FROM\n" +
                                    "          ubi_w.soj_page_trfc_w\n" +
                                    "        WHERE\n" +
                                    "          dt = '%s'\n" +
                                    "          GROUP BY page_id\n" +
                                    "      ) t\n" +
                                    "    WHERE\n" +
                                    "      t.page_id NOT IN (\n" +
                                    "        SELECT\n" +
                                    "          DISTINCT page_id\n" +
                                    "        from\n" +
                                    "          GDW_TABLES.DW_SOJ_LKP_PAGE\n" +
                                    "      )\n" +
                                    "  ) t1\n" +
                                    "  JOIN (\n" +
                                    "    select\n" +
                                    "      DISTINCT PAGE_ID,\n" +
                                    "      sojlib.soj_nvl(CLIENT_DATA, 'TPool') pool_name\n" +
                                    "    FROM\n" +
                                    "      UBI_V.UBI_EVENT\n" +
                                    "    WHERE\n" +
                                    "      SESSION_START_DT = '%s'\n" +
                                    "  ) t2 ON t1.page_id = t2.page_id";

        String dateString = localDate.toString();
        String sql = String.format(sparkSqlTemplate, dt, dateString);
        Dataset<Row> dataset = spark.sql(sql);
        List<Row> rows = dataset.collectAsList();

        List<PagePoolMapping> pagePoolMappingList = rows.parallelStream().map(row -> {
            int pageId = row.getInt(0);
            long traffic = row.getLong(1);
            String poolName = row.getString(2);
            PagePoolMapping pagePoolMapping = new PagePoolMapping();
            pagePoolMapping.setPageId(pageId);
            pagePoolMapping.setTraffic(traffic);
            pagePoolMapping.setPoolName(poolName);
            return pagePoolMapping;
        }).collect(Collectors.toList());

        cleanUpData(dateString);
        saveToMySQL(pagePoolMappingList, dateString);


        Dataset<Row> rowDataset = spark.createDataFrame(pagePoolMappingList, PagePoolMapping.class);
        try {
            rowDataset.registerTempTable("page_pool_view");
            //spark.sql("select * from page_pool_view").show();
            String insertSql = "INSERT OVERWRITE TABLE ubi_w.tdq_page_metadata_quality_w partition(dt = '%s')\n" +
                    "SELECT\n" +
                    "  pageId,\n" +
                    "  traffic,\n" +
                    "  poolName\n" +
                    "FROM\n" +
                    "  page_pool_view";
            String actualSQL = String.format(insertSql, dateString);
            spark.sql(actualSQL);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        //rowDataset.write().mode(SaveMode.Append).option("path", "viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/tdq/tdq_page_metadata_quality_w").insertInto("ubi_w.tdq_page_metadata_quality_w");
    }

    private void cleanUpData(String dateString) {
        try {
            Connection connection =  PipelineFactory.getInstance().getMySQLConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("delete from w_page_pool_lkp where dt = ?");
            preparedStatement.setString(1, dateString);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void saveToMySQL(List<PagePoolMapping> pagePoolMappingList, String dateString) {
        try {
            Connection connection = PipelineFactory.getInstance().getMySQLConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement("insert into w_page_pool_lkp(page_id, traffic, pool_name, dt) value (?, ?, ?, ?)");
            for (PagePoolMapping pagePoolMapping : pagePoolMappingList) {
                preparedStatement.setLong(1, pagePoolMapping.getPageId());
                preparedStatement.setLong(2, pagePoolMapping.getTraffic());
                preparedStatement.setString(3, pagePoolMapping.getPoolName());
                preparedStatement.setString(4, dateString);
                preparedStatement.addBatch();
            }
            int[] results = preparedStatement.executeBatch();
            System.out.println(Arrays.toString(results));
            connection.commit();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
            logger.error("insert into page_pool_tbl occurs exception: {0}", e);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private List<Long> listUnregisteredPageIds(LocalDate localDate) {
        try {
            Connection connection = PipelineFactory.getInstance().getMySQLConnection();
            String sqlTemplate = "select\n" +
                    "  page_id\n" +
                    "from\n" +
                    "  (\n" +
                    "    select\n" +
                    "      page_id\n" +
                    "    from\n" +
                    "      profiling_page_count\n" +
                    "    where\n" +
                    "      dt = '%s'\n" +
                    "    union\n" +
                    "    select\n" +
                    "      page_id\n" +
                    "    from\n" +
                    "      profiling_page_count_bot\n" +
                    "    where\n" +
                    "      dt = '%s'\n" +
                    "  ) t\n" +
                    "where\n" +
                    "  page_id not in (\n" +
                    "    select\n" +
                    "      page_id\n" +
                    "    from\n" +
                    "      profiling_page_lkp\n" +
                    "  )";
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            String dt = dateTimeFormatter.format(localDate);
            String actualSql = String.format(sqlTemplate, dt, dt);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(actualSql);
            List<Long> pageIds = new ArrayList<>();
            while (resultSet.next()){
                long pageId = resultSet.getLong("page_id");
                pageIds.add(pageId);
            }
            resultSet.close();
            statement.close();
            connection.close();
            return pageIds;
        } catch (Exception e) {
            logger.error("listUnwantedPageIds occurs exception: {0}", e);
            throw new RuntimeException(e);
        }
    }

    private List<Long> listUnregisteredPageIdsFromHive(LocalDate localDate) {
        try {
            SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
            String sqlTemplate = "SELECT\n" +
                    "  page_id\n" +
                    "FROM\n" +
                    "  (\n" +
                    "    SELECT\n" +
                    "      page_id,\n" +
                    "      sum(event_cnt)\n" +
                    "    FROM\n" +
                    "      ubi_w.soj_page_trfc_w\n" +
                    "    WHERE\n" +
                    "      dt = '%s'\n" +
                    "      GROUP BY page_id\n" +
                    "  ) t\n" +
                    "WHERE\n" +
                    "  t.page_id NOT IN (\n" +
                    "    SELECT\n" +
                    "      DISTINCT page_id\n" +
                    "    from\n" +
                    "      GDW_TABLES.DW_SOJ_LKP_PAGE\n" +
                    "  )";
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            String dt = dateTimeFormatter.format(localDate);
            String actualSql = String.format(sqlTemplate, dt);
            Dataset<Row> dataset = spark.sql(actualSql);
            List<Row> rows = dataset.collectAsList();
            if (rows.isEmpty()){
               return Collections.emptyList();
            }
            return rows.stream().map(row -> Long.valueOf(row.getInt(0))).collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("listUnregisteredPageIdsFromHive occurs exception: {0}", e);
            throw new RuntimeException(e);
        }
    }
}
