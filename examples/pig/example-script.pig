-- CassandraStorage
rows = LOAD 'cassandra://MyKeyspace/MyColumnFamily' USING CassandraStorage();
cols = FOREACH rows GENERATE flatten(columns);
colnames = FOREACH cols GENERATE $0;
namegroups = GROUP colnames BY (chararray) $0;
namecounts = FOREACH namegroups GENERATE COUNT($1), group;
orderednames = ORDER namecounts BY $0 DESC;
topnames = LIMIT orderednames 50;
dump topnames;

-- CqlStorage
libdata = LOAD 'cql://libdata/libout' USING CqlStorage();
book_by_mail = FILTER libdata BY C_OUT_TY == 'BM';

libdata_buildings = FILTER libdata BY SQ_FEET > 0;
state_flat = FOREACH libdata_buildings GENERATE STABR AS State,SQ_FEET AS SquareFeet;
state_grouped = GROUP state_flat BY State;
state_footage = FOREACH state_grouped GENERATE GROUP AS State, SUM(state_flat.SquareFeet) AS TotalFeet:int;

insert_format= FOREACH state_footage GENERATE TOTUPLE(TOTUPLE('year',2011),TOTUPLE('state',State)),TOTUPLE(TotalFeet);
STORE insert_format INTO 'cql://libdata/libsqft?output_query=UPDATE%20libdata.libsqft%20SET%20sqft%20%3D%20%3F' USING CqlStorage;