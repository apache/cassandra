CREATE KEYSPACE excalibur
    WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3};

DESCRIBE KEYSPACE excalibur;
