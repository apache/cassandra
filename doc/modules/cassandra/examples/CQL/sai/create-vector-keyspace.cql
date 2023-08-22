CREATE KEYSPACE IF NOT EXISTS vsearch 
   WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };