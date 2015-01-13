moretestvalues= LOAD 'cql://cql3ks/moredata/' USING CqlNativeStorage;
insertformat= FOREACH moretestvalues GENERATE TOTUPLE(TOTUPLE('a',x)),TOTUPLE(y);
STORE insertformat INTO 'cql://cql3ks/test?output_query=UPDATE+cql3ks.test+set+b+%3D+%3F' USING CqlNativeStorage;

-- composite key
moredata = load 'cql://cql3ks/compmore' USING CqlNativeStorage;
insertformat = FOREACH moredata GENERATE TOTUPLE (TOTUPLE('a',x),TOTUPLE('b',y), TOTUPLE('c',z)),TOTUPLE(data);
STORE insertformat INTO 'cql://cql3ks/compotable?output_query=UPDATE%20cql3ks.compotable%20SET%20d%20%3D%20%3F' USING CqlNativeStorage;

-- collection column
collectiontable = LOAD 'cql://cql3ks/collectiontable/' USING CqlNativeStorage;
-- recs= (((m,kk)),((map,(m,mm),(n,nn))))
recs= FOREACH collectiontable GENERATE TOTUPLE(TOTUPLE('m', m) ), TOTUPLE(TOTUPLE('map', TOTUPLE('m', 'mm'), TOTUPLE('n', 'nn')));
store recs INTO 'cql://cql3ks/collectiontable?output_query=update+cql3ks.collectiontable+set+n+%3D+%3F' USING CqlNativeStorage();
