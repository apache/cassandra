# Cassandra Installation

- Docker based installation
  ```bash
  $ docker pull cassandra:2.2
  $ docker run -d --name kong-database -p 9042:9042 cassandra:2.2
  ```

- Query Test
  - Verify if Cassandra is running; run cassandra shell prompt using:
  ```bash
  $ docker exec -it kong-database bash

  $ nodetool status
    Datacenter: datacenter1
    =======================
    Status=Up/Down
    |/ State=Normal/Leaving/Joining/Moving
    --  Address     Load        Tokens       Owns (effective)  Host ID                               Rack
    UN  172.18.0.2  205.18 KiB  256          100.0%            42022df6-9d27-4b84-a193-63adb1c221e5  rack1
  
  $ cqlsh
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh 5.0.1 | Cassandra 3.11.10 | CQL spec 3.4.4 | Native protocol v4]
    Use HELP for help.
    cqlsh> 
  ```

  - Make a table
  ```bash
    create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
    create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
    create index on example.tweet(timeline);
  ```

## Reference
- [kong and cassandra deployment](https://wenchma.github.io/2016/08/01/kong-and-cassandra-deployment.html)