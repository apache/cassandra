CREATE KEYSPACE excalibur
   WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3, 'DC2': 2};

DESCRIBE KEYSPACE excalibur;
