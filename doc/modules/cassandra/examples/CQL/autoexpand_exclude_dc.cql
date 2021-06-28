CREATE KEYSPACE excalibur
   WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3, 'DC2': 0};

DESCRIBE KEYSPACE excalibur;
