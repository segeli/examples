package io.github.streamingwithflink.conversion;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FacebookConversionStreamApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tEnv.executeSql("CREATE TABLE add_to_carts (" +
                "    channel STRING, " +
                "    deviceId STRING, " +
                "    sessionId STRING, " +
                "    eventId STRING, " +
                "    eventTime BIGINT, " +
                "    eventName STRING, " +
                //"    params ROW(`sellerId` STRING, `quantity` STRING), " +
                "    params MAP<STRING, STRING>, " +
                "    rowTime AS CAST(eventTime AS TIMESTAMP(3)), " +
                "    WATERMARK FOR rowTime AS rowTime - INTERVAL '15' SECOND" +
                ") WITH ( " +
                "    'connector.type' = 'kafka', " +
                "    'connector.version' = 'universal', " +
                "    'connector.topic' = 'addToCart', " +
                "    'connector.startup-mode' = 'earliest-offset', " +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181', " +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'format.type' = 'json'" +
                ")");

        tEnv.executeSql("CREATE TABLE session_starts (" +
                "    channel STRING, " +
                "    deviceId STRING, " +
                "    sessionId STRING, " +
                "    eventId STRING, " +
                "    eventTime BIGINT, " +
                "    eventName STRING, " +
                "    params MAP<STRING, STRING>, " +
                "    rowTime AS CAST(eventTime AS TIMESTAMP(3)), " +
                "    WATERMARK FOR rowTime AS rowTime" +
                ") WITH ( " +
                "    'connector.type' = 'kafka', " +
                "    'connector.version' = 'universal', " +
                "    'connector.topic' = 'sessionStart', " +
                "    'connector.startup-mode' = 'earliest-offset', " +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181', " +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'format.type' = 'json'" +
                ")");

        tEnv.executeSql("CREATE VIEW versioned_session_starts AS " +
                "SELECT sessionId, params, rowTime FROM ( " +
                "SELECT *, ROW_NUMBER() OVER (PARTITION BY sessionId ORDER BY rowTime DESC) AS rowNum " +
                "FROM session_starts ) " +
                "WHERE rowNum = 1");

        Table table = tEnv.sqlQuery("SELECT" +
                " add_to_carts.channel AS channel, " +
                " add_to_carts.deviceId AS deviceId, " +
                " add_to_carts.sessionId AS sessionId, " +
                " CAST(versioned_session_starts.params['userAgent'] AS VARCHAR) AS userAgent, " +
                " CAST(versioned_session_starts.params['deviceInfo'] AS VARCHAR) AS deviceInfo, " +
                " CAST(versioned_session_starts.params['referrer'] AS VARCHAR) AS referrer, " +
                " CAST(add_to_carts.params['quantity'] AS VARCHAR) AS quantity, " +
                " CAST(add_to_carts.params['categoryId'] AS VARCHAR) AS categoryId, " +
                " CAST(add_to_carts.params['categoryName'] AS VARCHAR) AS categoryName, " +
                " CAST(add_to_carts.params['sellerId'] AS VARCHAR) AS sellerId, " +
                " CAST(add_to_carts.params['categoryUrl'] AS VARCHAR) AS categoryUrl, " +
                " CAST(add_to_carts.params['productId'] AS VARCHAR) AS productId, " +
                " CAST(add_to_carts.params['skuId'] AS VARCHAR) AS skuId, " +
                " CAST(add_to_carts.params['discountedPrice'] AS VARCHAR) AS discountedPrice, " +
                " CAST(add_to_carts.params['price'] AS VARCHAR) AS price" +
                " FROM add_to_carts " +
                " LEFT JOIN versioned_session_starts FOR SYSTEM_TIME AS OF add_to_carts.rowTime " +
                " ON add_to_carts.sessionId = versioned_session_starts.sessionId");
        TableResult execute = table.execute();
        execute.print();
    }
}
