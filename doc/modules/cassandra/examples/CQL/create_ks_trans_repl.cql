CREATE KEYSPACE some_keyspace
   WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : '3/1'', 'DC2' : '5/2'};
