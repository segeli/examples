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
                "    'connector.startup-mode' = 'latest-offset', " +
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
                "    PRIMARY KEY(sessionId) NOT ENFORCED, " +
                "    WATERMARK FOR rowTime AS rowTime" +
                ") WITH ( " +
                "    'connector.type' = 'kafka', " +
                "    'connector.version' = 'universal', " +
                "    'connector.topic' = 'sessionStart', " +
                "    'connector.startup-mode' = 'latest-offset', " +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181', " +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092'," +
                "    'format.type' = 'json'" +
                ")");

        Table table = tEnv.sqlQuery("SELECT" +
                " add_to_carts.params, " +
                " session_starts.params " +
                " FROM add_to_carts " +
                " LEFT JOIN session_starts FOR SYSTEM_TIME AS OF add_to_carts.rowTime " +
                " ON add_to_carts.sessionId = session_starts.sessionId");
        TableResult execute = table.execute();
        execute.print();
    }
}
