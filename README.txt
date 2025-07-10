https://hub.docker.com/r/apache/kafka

from directory containing docker-compose.yml:

docker compose up --build
docker exec --workdir /opt/kafka/bin/ -it kafka sh
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic

# This command will wait for input at a > prompt. Enter hello, press Enter, then world, and press Enter again. Enter Ctrl+C to exit the console producer.
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic

./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning

stop and remove the container by running the following command on your host machine from the directory containing the docker-compose.yml file:

docker compose down


