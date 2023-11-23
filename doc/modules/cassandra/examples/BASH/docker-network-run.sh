docker network create cassandra

docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra