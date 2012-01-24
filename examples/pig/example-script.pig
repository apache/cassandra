rows = LOAD 'cassandra://MyKeyspace/MyColumnFamily' USING CassandraStorage() AS (key, columns: bag {T: tuple(name, value)});
cols = FOREACH rows GENERATE flatten(columns);
colnames = FOREACH cols GENERATE $0;
namegroups = GROUP colnames BY (chararray) $0;
namecounts = FOREACH namegroups GENERATE COUNT($1), group;
orderednames = ORDER namecounts BY $0;
topnames = LIMIT orderednames 50;
dump topnames;