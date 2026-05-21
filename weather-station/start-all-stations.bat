@echo off
echo Starting 10 weather stations...

start "Station 1"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 1  localhost:9092
start "Station 2"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 2  localhost:9092
start "Station 3"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 3  localhost:9092
start "Station 4"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 4  localhost:9092
start "Station 5"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 5  localhost:9092
start "Station 6"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 6  localhost:9092
start "Station 7"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 7  localhost:9092
start "Station 8"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 8  localhost:9092
start "Station 9"  java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 9  localhost:9092
start "Station 10" java -jar target\weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar 10 localhost:9092

echo All 10 stations started!