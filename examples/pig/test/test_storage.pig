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


--
--  Test counter column family support
--
CC = load 'cassandra://PigTest/CC' using CassandraStorage();

total_hits = foreach CC generate key, SUM(columns.value);

dump total_hits;

--
--  Test CompositeType
--

compo = load 'cassandra://PigTest/Compo' using CassandraStorage();

compo = foreach compo generate key as method, flatten(columns);

lee = filter compo by columns::name == ('bruce','lee');

dump lee;

night = load 'cassandra://PigTest/CompoInt' using CassandraStorage();
night = foreach night generate flatten(columns);
night = foreach night generate (int)columns::name.$0+(double)columns::name.$1/60 as hour, columns::value as noise;

-- What happens at the darkest hour?
darkest = filter night by hour > 2 and hour < 5;

dump darkest;

compo_int_rows = LOAD 'cassandra://PigTest/CompoInt' USING CassandraStorage();
STORE compo_int_rows INTO 'cassandra://PigTest/CompoIntCopy' USING CassandraStorage();

--
--  Test CompositeKey
--

compokeys = load 'cassandra://PigTest/CompoKey' using CassandraStorage();
compokeys = filter compokeys by key.$1 == 40;

dump compokeys;

compo_key_rows = LOAD 'cassandra://PigTest/CompoKey' USING CassandraStorage();
STORE compo_key_rows INTO 'cassandra://PigTest/CompoKeyCopy' USING CassandraStorage();
