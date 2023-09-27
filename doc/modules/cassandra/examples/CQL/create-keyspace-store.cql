CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = 
{ 'class' : 'SimpleStrategy', 
'replication_factor' : '1' 
};