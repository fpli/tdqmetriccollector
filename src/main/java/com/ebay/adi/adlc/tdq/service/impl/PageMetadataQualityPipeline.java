package com.ebay.adi.adlc.tdq.service.impl;

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
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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
        SparkSession spark = SparkSessionStore.getInstance().getSparkSession();
        String sql = "";
        Dataset<Row> dataset = spark.sql(sql);

    }

    private List<Long> listUnwantedPageIds(LocalDate localDate){
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
            return pageIds;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
