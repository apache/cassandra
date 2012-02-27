rows = LOAD 'cassandra://PigTest/SomeApp' USING CassandraStorage();
-- full copy
STORE rows INTO 'cassandra://PigTest/CopyOfSomeApp' USING CassandraStorage();
-- single tuple
onecol = FOREACH rows GENERATE key, percent;
STORE onecol INTO 'cassandra://PigTest/CopyOfSomeApp' USING CassandraStorage();
-- bag only
other = FOREACH rows GENERATE key, columns;
STORE other INTO 'cassandra://PigTest/CopyOfSomeApp' USING CassandraStorage();


-- filter
likes = FILTER rows by vote_type.value eq 'like' and rating.value > 5;
dislikes_extras = FILTER rows by vote_type.value eq 'dislike' AND COUNT(columns) > 0;

-- store these too
STORE likes INTO 'cassandra://PigTest/CopyOfSomeApp' USING CassandraStorage();
STORE dislikes_extras INTO 'cassandra://PigTest/CopyOfSomeApp' USING CassandraStorage();

-- filter to fully visible rows (no uuid columns) and dump
visible = FILTER rows BY COUNT(columns) == 0;
dump visible;



-- test key types with a join
U8 = load 'cassandra://PigTest/U8' using CassandraStorage();
Bytes = load 'cassandra://PigTest/Bytes' using CassandraStorage();

-- cast key to chararray
b = foreach Bytes generate (chararray)key, columns;

-- key in Bytes is a bytearray, U8 chararray
a = join Bytes by key, U8 by key;
dump a

-- key should now be cast into a chararray
c = join b by (chararray)key, U8 by (chararray)key;
dump c
