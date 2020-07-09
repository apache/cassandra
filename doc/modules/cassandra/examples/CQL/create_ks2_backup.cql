CREATE KEYSPACE catalogkeyspace
   WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
