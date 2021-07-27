# Kong Installation via Docker

## Installation w/ DB

- Create a Docker network
  ```
  $ docker network create kong-net
  ```

- Start DB
  - If you wish to use a Cassandra container:
  ```bash
  $ docker run -d --name kong-database \
               --network=kong-net \
               -p 9042:9042 \
               cassandra:3
  ```
  - If you wish to use a PostgreSQL container:
  ```bash
  $ docker run -d --name kong-database \
               --network=kong-net \
               -p 5432:5432 \
               -e "POSTGRES_USER=kong" \
               -e "POSTGRES_DB=kong" \
               -e "POSTGRES_PASSWORD=kong" \
               postgres:9.6
  ```

- Prepare DB
  Run the migrations with an ephemeral Kong container:
  ```bash
  $ docker run --rm \
      --network=kong-net \
      -e "KONG_DATABASE=cassandra" \
      -e "KONG_PG_HOST=kong-database" \
      -e "KONG_PG_USER=kong" \
      -e "KONG_PG_PASSWORD=kong" \
      -e "KONG_CASSANDRA_CONTACT_POINTS=kong-database" \
      kong:latest kong migrations bootstrap
  ```
  In the above example, both Cassandra and PostgreSQL are configured, but you should update the KONG_DATABASE environment variable with either cassandra or postgres.

- Start Kong

  When the migrations have run and your database is ready, start a Kong container that will connect to your database container, just like the ephemeral migrations container:

  ```bash
  $ docker run -d --name kong \
     --network=kong-net \
     -e "KONG_DATABASE=cassandra" \
     -e "KONG_PG_HOST=kong-database" \
     -e "KONG_PG_USER=kong" \
     -e "KONG_PG_PASSWORD=kong" \
     -e "KONG_CASSANDRA_CONTACT_POINTS=kong-database" \
     -e "KONG_PROXY_ACCESS_LOG=/dev/stdout" \
     -e "KONG_ADMIN_ACCESS_LOG=/dev/stdout" \
     -e "KONG_PROXY_ERROR_LOG=/dev/stderr" \
     -e "KONG_ADMIN_ERROR_LOG=/dev/stderr" \
     -e "KONG_ADMIN_LISTEN=0.0.0.0:8001, 0.0.0.0:8444 ssl" \
     -p 8000:8000 \
     -p 8443:8443 \
     -p 127.0.0.1:8001:8001 \
     -p 127.0.0.1:8444:8444 \
     kong:latest
  ```

- Use Kong

  Kong is running
  ```bash
  $ curl -i http://localhost:8001/
  ```

## Reference 
- [Kong: Docker Installation](https://docs.konghq.com/install/docker/)
