Testing : 
cd C:\Users\USER\OneDrive\Documents\Uni\term8\Dataintensive\labs\Final_Project\weather-monitoring\weather-station
start-all-stations.bat
docker exec -it weather-monitoring-kafka-1 bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic weather-stations --from-beginning | grep station_id | head -50
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my_first