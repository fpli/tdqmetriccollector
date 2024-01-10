package com.ebay.adi.adlc.tdq.service.impl;

import com.ebay.adi.adlc.tdq.pojo.PagePoolMapping;
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
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        List<Long> pageIds = listUnwantedPageIds(localDate);
        Stream<String> pageIdStream = pageIds.stream().map(String::valueOf);
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
        String sparkSqlTemplate = "select\n" +
                "  DISTINCT PAGE_ID, sojlib.soj_nvl(CLIENT_DATA, 'TPool')\n" +
                "FROM\n" +
                "  UBI_V.UBI_EVENT\n" +
                "WHERE\n" +
                "  SESSION_START_DT = '%s'\n" +
                "  AND PAGE_ID IN (%s)";
        StringJoiner stringJoiner = new StringJoiner(",");
        pageIdStream.forEach(stringJoiner::add);
        String pageIdString = stringJoiner.toString();
        String sql = String.format(sparkSqlTemplate, localDate.toString(), pageIdString);
        Dataset<Row> dataset = spark.sql(sql);
        List<Row> rows = dataset.collectAsList();

        List<PagePoolMapping> pagePoolMappingList = rows.parallelStream().map(row -> {
            long pageId = row.getInt(0);
            String poolName = row.getString(1);
            return new PagePoolMapping(pageId, poolName);
        }).collect(Collectors.toList());

        persistence(pagePoolMappingList);
    }

    private void persistence(List<PagePoolMapping> pagePoolMappingList) {
        try {
            Connection connection = PipelineFactory.getInstance().getMySQLConnection();
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement("insert into page_pool_tbl(page_id, pool_name) value (?, ?)");
            for (PagePoolMapping pagePoolMapping : pagePoolMappingList) {
                preparedStatement.setLong(1, pagePoolMapping.getPageId());
                preparedStatement.setString(2, pagePoolMapping.getPoolName());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
            preparedStatement.close();
            connection.close();
        } catch (Exception e) {
            logger.error("insert into page_pool_tbl occurs exception: {0}", e);
            throw new RuntimeException(e);
        }
    }

    private List<Long> listUnwantedPageIds(LocalDate localDate) {
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
                    "      pa ge_id\n" +
                    "    from\n" +
                    "      profiling_page_lkp\n" +
                    "  )";
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            String dt = dateTimeFormatter.format(localDate);
            String actualSql = String.format(sqlTemplate, dt);
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
}
