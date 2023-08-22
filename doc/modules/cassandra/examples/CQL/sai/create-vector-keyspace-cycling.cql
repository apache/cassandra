CREATE KEYSPACE IF NOT EXISTS cycling 
   WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };