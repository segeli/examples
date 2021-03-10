package io.github.streamingwithflink.conversion;

import io.github.streamingwithflink.conversion.config.EventDeserializer;
import io.github.streamingwithflink.conversion.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class FacebookConversionStreamApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "events");

        FlinkKafkaConsumer<Event> addToCartKafkaSource =
                new FlinkKafkaConsumer<>("addToCart", new EventDeserializer(), properties);
        addToCartKafkaSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));
        DataStream<Event> addToCartStream = bsEnv
                .addSource(addToCartKafkaSource)
                .returns(TypeInformation.of(Event.class));
        DataStream<Tuple4<String, Timestamp, String, String>> returnAddToCartStream = addToCartStream
                .map(new MapToAddToCartTuple());
        Table addToCartStreamTable =
                tEnv.fromDataStream(returnAddToCartStream,
                        $("f0").as("sessionId"),
                        $("f1").as("eventTime"),
                        $("f2").as("sellerId"),
                        $("f3").as("quantity"));
        tEnv.registerTable("addToCarts", addToCartStreamTable);

        FlinkKafkaConsumer<Event> sessionStartKafkaSource = new FlinkKafkaConsumer<>("sessionStart", new EventDeserializer(), properties);
        sessionStartKafkaSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));
        DataStream<Event> sessionStartStream = bsEnv
                .addSource(sessionStartKafkaSource)
                .returns(TypeInformation.of(Event.class));
        DataStream<Tuple4<String, Timestamp, String, String>> returnSessionStartStream = sessionStartStream
                .map(new MapToSessionStartTuple());

        // Create and register an example table using the sample data set.
        Table sessionStartHistoryTable =
                tEnv.fromDataStream(returnSessionStartStream, $("sessionId"), $("userAgent"), $("deviceInfo"), $("eventTime"));
        tEnv.registerTable("sessionStartHistory", sessionStartHistoryTable);

        // Create and register the temporal table function "sessionStarts".
        // Define "eventTime" as the versioning field and "sessionId" as the primary key.
        TemporalTableFunction sessionStarts =
                sessionStartHistoryTable.createTemporalTableFunction($("eventTime"), $("sessionId"));
        tEnv.registerFunction("sessionStarts", sessionStarts);

        String query = "SELECT ss.sessionId, ss.userAgent, ss.deviceInfo, atc.sellerId, atc.quantity " +
                "FROM addToCarts AS atc LEFT JOIN LATERAL TABLE(sessionStarts(atc.eventTime)) AS ss ON TRUE " +
                "WHERE atc.sessionId = ss.sessionId";

        Table table = tEnv.sqlQuery(query);
        TableResult execute = table.execute();
        execute.print();

    }

    private static class MapToSessionStartTuple implements MapFunction<Event, Tuple4<String, Timestamp, String, String>> {
        @Override
        public Tuple4<String, Timestamp, String, String> map(Event event) throws Exception {
            return Tuple4.of(event.getSessionId(),
                    new Timestamp(event.getEventTime() * 1000),
                    event.getParams().get("userAgent"),
                    event.getParams().get("deviceInfo"));
        }
    }

    private static class MapToAddToCartTuple implements MapFunction<Event, Tuple4<String, Timestamp, String, String>> {
        @Override
        public Tuple4<String, Timestamp, String, String> map(Event event) throws Exception {
            return Tuple4.of(event.getSessionId(),
                    new Timestamp(event.getEventTime() * 1000),
                    event.getParams().get("sellerId"),
                    event.getParams().get("quantity"));
        }
    }
}
