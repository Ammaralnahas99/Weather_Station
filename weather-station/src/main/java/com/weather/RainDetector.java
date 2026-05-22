package com.weather;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

public class RainDetector {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        KStream<String, String> weatherStream = builder.stream("weather-readings");

        weatherStream
            .filter((key, value) -> {
                try {
                    var node = mapper.readTree(value);
                    int humidity = node.path("weather").path("humidity").asInt();
                    return humidity > 70;
                } catch (Exception e) {
                    return false;
                }
            })
            .mapValues(value -> {
                try {
                    var node = mapper.readTree(value);
                    long stationId = node.path("station_id").asLong();
                    int humidity = node.path("weather").path("humidity").asInt();
                    return String.format(
                        "{\"alert\":\"RAINING\",\"station_id\":%d,\"humidity\":%d}",
                        stationId, humidity
                    );
                } catch (Exception e) {
                    return value;
                }
            })
            .to("rain-alerts");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}