rows = LOAD 'cassandra://Keyspace1/Standard1' USING CassandraStorage();
cols = FOREACH rows GENERATE flatten($1);
colnames = FOREACH cols GENERATE $0;
namegroups = GROUP colnames BY $0;
namecounts = FOREACH namegroups GENERATE COUNT($1), group;
orderednames = ORDER namecounts BY $0;
topnames = LIMIT orderednames 50;
dump topnames;
