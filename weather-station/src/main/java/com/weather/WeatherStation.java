package com.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class WeatherStation {

    // Station ID is passed as a command-line argument
    private final long stationId;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();

    // Battery distribution: low=30%, medium=40%, high=30%
    private static final String[] BATTERY_LEVELS = {
        "low", "low", "low",
        "medium", "medium", "medium", "medium",
        "high", "high", "high"
    };

    public WeatherStation(long stationId, String kafkaBootstrap) {
        this.stationId = stationId;

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void start() throws Exception {
        long sequenceNumber = 1;

        System.out.println("Station " + stationId + " starting...");

        while (true) {
            // 10% drop rate — simply skip sending
            if (random.nextInt(10) == 0) {
                System.out.println("Station " + stationId + " dropped message #" + sequenceNumber);
                sequenceNumber++;
                Thread.sleep(1000);
                continue;
            }

            // Build the message
            WeatherMessage msg = buildMessage(sequenceNumber);
            String json = mapper.writeValueAsString(msg);

            // Send to Kafka topic "weather-stations"
            ProducerRecord<String, String> record =
                new ProducerRecord<>("weather-stations",
                    String.valueOf(stationId), json);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending: " + exception.getMessage());
                }
            });

            System.out.println("Station " + stationId + " sent #" + sequenceNumber);
            sequenceNumber++;
            Thread.sleep(1000);
        }
    }

    private WeatherMessage buildMessage(long sNo) {
        String battery = BATTERY_LEVELS[random.nextInt(BATTERY_LEVELS.length)];
        long timestamp = System.currentTimeMillis() / 1000;

        WeatherMessage.Weather weather = new WeatherMessage.Weather(
            random.nextInt(100),       // humidity 0-100%
            random.nextInt(60) + 60,   // temperature 60-120°F
            random.nextInt(50)         // wind speed 0-50 km/h
        );

        return new WeatherMessage(stationId, sNo, battery, timestamp, weather);
    }

    public static void main(String[] args) throws Exception {
        // Usage: java WeatherStation <station_id> <kafka_bootstrap>
        long stationId = args.length > 0 ? Long.parseLong(args[0]) : 1;
        String kafka   = args.length > 1 ? args[1] : "localhost:9092";

        new WeatherStation(stationId, kafka).start();
    }
}