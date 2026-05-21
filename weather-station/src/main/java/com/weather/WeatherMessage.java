package com.weather;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherMessage {

    @JsonProperty("station_id")
    public long stationId;

    @JsonProperty("s_no")
    public long sNo;

    @JsonProperty("battery_status")
    public String batteryStatus;

    @JsonProperty("status_timestamp")
    public long statusTimestamp;

    @JsonProperty("weather")
    public Weather weather;

    public WeatherMessage() {}

    public WeatherMessage(long stationId, long sNo, String batteryStatus,
                          long statusTimestamp, Weather weather) {
        this.stationId = stationId;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public static class Weather {
        @JsonProperty("humidity")
        public int humidity;

        @JsonProperty("temperature")
        public int temperature;

        @JsonProperty("wind_speed")
        public int windSpeed;

        public Weather() {}

        public Weather(int humidity, int temperature, int windSpeed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.windSpeed = windSpeed;
        }
    }
}