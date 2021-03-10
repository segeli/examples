package io.github.streamingwithflink.conversion.generator;

import com.github.javafaker.Faker;
import io.github.streamingwithflink.conversion.config.EventSerializer;
import io.github.streamingwithflink.conversion.model.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class EventGenerator {

    private static final List<Category> categories;
    private static final List<String> sellerIds;
    private static final Map<String, String> browsers;

    static {
        categories = new ArrayList<>();
        categories.add(new Category("1156152", "Çaydanlık", "/mutfak-gerecleri/cay-kahve-demleme/caydanlik"));

        sellerIds = new ArrayList<>();
        sellerIds.add("111");
        sellerIds.add("222");
        sellerIds.add("333");
        sellerIds.add("444");
        sellerIds.add("555");

        browsers = new HashMap<>();
        browsers.put("111", "Chrome");
        browsers.put("222", "Mozilla");
        browsers.put("333", "IE");
        browsers.put("444", "Chrome/79.0.3945.88");
        browsers.put("555", "Safari/537.36");
    }

    public static void main(String[] args) {
        Faker faker = new Faker();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        for (String sellerId : sellerIds) {
            Map<String, String> sessionStartParams = new HashMap<>();
            sessionStartParams.put("userAgent", browsers.get(sellerId));
            sessionStartParams.put("deviceInfo", "{\"osVersion\":\"Mac OS 10.14.6\",\"brandName\":\"Chrome\",\"model\":\"79.0.3945.88\"}");
            sessionStartParams.put("referrer", "");

            String sessionId = getUUIDToString();
            System.out.printf("Seller Id: %s SessionId: %s", sellerId, sessionId);
            String deviceId = getUUIDToString();
            Event sessionStart = new Event("n11", "WEB", deviceId, sessionId, getUUIDToString(), Instant.now().getEpochSecond(), "sessionStart");
            sessionStart.setParams(sessionStartParams);

            ProducerRecord<Object, Object> sessionStartRecord = new ProducerRecord<>("sessionStart", sessionStart);
            producer.send(sessionStartRecord);
            producer.flush();

            int upperbound = 5;
            int sizeOfCartItems = new Random().nextInt(upperbound);
            for (int i = 0; i < sizeOfCartItems; i++) {
                Event addToCart = new Event("n11", "WEB", deviceId, sessionId, getUUIDToString(), Instant.now().getEpochSecond(), "addToCart");

                Map<String, String> addToCartParams = new HashMap<>();
                addToCartParams.put("quantity", "1");
                addToCartParams.put("categoryId", categories.get(0).getId());
                addToCartParams.put("categoryName", categories.get(0).getName());
                addToCartParams.put("categoryUrl", categories.get(0).getUrl());
                addToCartParams.put("sellerId", sellerId);
                addToCartParams.put("productId", faker.commerce().productName());
                String price = faker.commerce().price();
                addToCartParams.put("price", price);
                addToCartParams.put("discountedPrice", price);
                addToCart.setParams(addToCartParams);

                ProducerRecord<Object, Object> addToCartRecord = new ProducerRecord<>("addToCart", addToCart);
                producer.send(addToCartRecord);
                producer.flush();
            }
        }

        producer.close();
    }

    private static String getUUIDToString() {
        return UUID.randomUUID().toString();
    }
}
