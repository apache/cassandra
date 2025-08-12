/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class MutationTrackingRangeReadTest extends TestBaseImpl
{
    private static final int REPLICAS = 3;

    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException
    {
        cluster = Cluster.build()
                         .withNodes(REPLICAS)
                         .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP).set("mutation_tracking_enabled", "true"))
                         .start();
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    /*
     * Seed = 3448511221048049990
     * Examples = 1
     * Pure = true
     * Error: Unexpected results for query: SELECT * FROM ks1.tbl WHERE pk0 = 7137864754153440313 PER PARTITION LIMIT 21 LIMIT 914 ALLOW FILTERING
     * Steps: 400
     * Values:
     * 	State: 
     * 		Setup:
     * 		Config:
     * 		sstable:
     * 			selected_format: bti
     * 		CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';
     * 		CREATE TYPE IF NOT EXISTS ks1."tOGqK49dTH9n5nZhCHxpmKOPKYO8w4I04vHtYY41ys" (
     * 		    f0 boolean,
     * 		    f1 blob,
     * 		    f2 smallint
     * 		);;
     * 		CREATE TABLE ks1.tbl (
     * 		    pk0 bigint,
     * 		    pk1 text,
     * 		    ck0 bigint,
     * 		    s0 frozen<list<frozen<list<time>>>> static,
     * 		    v0 'org.apache.cassandra.db.marshal.LexicalUUIDType',
     * 		    v1 frozen<tuple<frozen<"tOGqK49dTH9n5nZhCHxpmKOPKYO8w4I04vHtYY41ys">, vector<timeuuid, 1>>>,
     * 		    PRIMARY KEY ((pk0, pk1), ck0)
     * 		) WITH CLUSTERING ORDER BY (ck0 DESC)
     * 		    AND read_repair = 'NONE'
     * 		    AND transactional_mode = 'off'
     * 		    AND transactional_migration_from = 'none'
     * 	History:
     * 		1: UPDATE ks1.tbl USING TIMESTAMP 1 SET s0=[['08:31:40.807720853', '01:14:50.323756148', '13:00:06.063871867']] WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲' -- on node3
     * 		2: UPDATE ks1.tbl USING TIMESTAMP 2 SET s0=[['03:28:16.047802044']] WHERE  pk0 = 7137864754153440313 AND  pk1 = '뢸镝蔥' -- on node1
     * 		3: INSERT INTO ks1.tbl (pk0, pk1, ck0, v0) VALUES (7137864754153440313, '뢸镝蔥', 7732824726196172505, 0x0000000000004d00af00000000000000) USING TIMESTAMP 3 -- on node2
     * 		4: SELECT * FROM ks1.tbl WHERE ck0 = 7732824726196172505 ALLOW FILTERING -- ck0 bigint (reversed), on node2
     * 		5: SELECT * FROM ks1.tbl WHERE s0 > [['03:28:16.047802044']] PER PARTITION LIMIT 519 LIMIT 721 ALLOW FILTERING -- s0 frozen<list<frozen<list<time>>>>, on node2, fetch size 1000
     * 		6: nodetool flush ks1 tbl
     * 		7: SELECT * FROM ks1.tbl WHERE pk0 = 7137864754153440313 AND pk1 = '뢸镝蔥' LIMIT 521 -- By Partition Key, on node2, fetch size 1
     * 		8: SELECT * FROM ks1.tbl WHERE pk0 = 1699976006349660742 AND pk1 = 'ጬ葲' LIMIT 486 -- By Partition Key, on node2, fetch size 5000
     * 		9: SELECT * FROM ks1.tbl -- full table scan, on node2
     * 		10: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(1699976006349660742, 'ጬ葲') AND token(pk0, pk1) <= token(7137864754153440313, '뢸镝蔥') -- by token range, on node3, fetch size 1000
     * 		11: SELECT * FROM ks1.tbl WHERE v0 = 0x0000000000004d00af00000000000000 ALLOW FILTERING -- v0 'org.apache.cassandra.db.marshal.LexicalUUIDType', on node2
     * 		12: INSERT INTO ks1.tbl (pk0, pk1, s0) VALUES (1699976006349660742, 'ጬ葲', null) USING TIMESTAMP 4 -- on node1
     * 		13: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) >= token(1699976006349660742, 'ጬ葲') AND token(pk0, pk1) < token(7137864754153440313, '뢸镝蔥') PER PARTITION LIMIT 402 -- by token range, on node1, fetch size 1
     * 		14: SELECT * FROM ks1.tbl LIMIT 980 -- full table scan, on node1
     * 		15: SELECT * FROM ks1.tbl WHERE pk0 = 7137864754153440313 AND pk1 = '뢸镝蔥' AND ck0 <= 7732824726196172505 PER PARTITION LIMIT 154 LIMIT 27 ALLOW FILTERING -- ck0 bigint (reversed), on node1
     * 		16: UPDATE ks1.tbl USING TIMESTAMP 5 SET s0=[['01:28:35.208066780', '05:25:43.184564123'], ['16:14:58.464860367', '13:59:53.463983006', '10:32:10.674489767']] WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲' -- on node2
     * 		17: SELECT * FROM ks1.tbl WHERE v0 <= 0x0000000000004d00af00000000000000 PER PARTITION LIMIT 39 ALLOW FILTERING -- v0 'org.apache.cassandra.db.marshal.LexicalUUIDType', on node1, fetch size 1
     * 		18: nodetool flush ks1 tbl
     * 		19: SELECT writetime(s0), writetime(v0), writetime(v1) FROM ks1.tbl
     * 		20: INSERT INTO ks1.tbl (pk0, pk1, s0) VALUES (7137864754153440313, '뢸镝蔥', [['11:13:31.615781929', '02:03:35.298191424', '21:32:35.861361643']]) USING TIMESTAMP 6 -- on node3
     * 		21: SELECT * FROM ks1.tbl WHERE v0 <= 0x0000000000004d00af00000000000000 PER PARTITION LIMIT 96 ALLOW FILTERING -- v0 'org.apache.cassandra.db.marshal.LexicalUUIDType', on node3
     * 		22: SELECT * FROM ks1.tbl WHERE ck0 >= 7732824726196172505 PER PARTITION LIMIT 334 LIMIT 596 ALLOW FILTERING -- ck0 bigint (reversed), on node1, fetch size 1
     * 		23: DELETE FROM ks1.tbl USING TIMESTAMP 7 WHERE  pk0 = -5694501802205955587 AND  pk1 = '䱔틊雬ⲓ텓┪炷ᱳ' AND  ck0 = -6329240054733066635 -- on node1
     * 		24: SELECT * FROM ks1.tbl WHERE pk0 <= 7137864754153440313 PER PARTITION LIMIT 223 LIMIT 958 ALLOW FILTERING -- pk0 bigint, on node3, fetch size 1
     * 		25: UPDATE ks1.tbl USING TIMESTAMP 8 SET s0=[['01:53:27.416986187', '06:59:06.972693101', '22:18:26.463792361'], ['10:28:11.888503614', '18:21:42.999485132']] WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲' -- on node3
     * 		26: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) BETWEEN 5433756588025747060 AND -9223372036854775808 PER PARTITION LIMIT 162 LIMIT 988 -- min token range, on node2, fetch size 1
     * 		27: SELECT * FROM ks1.tbl WHERE pk0 = 1699976006349660742 AND pk1 = 'ጬ葲' PER PARTITION LIMIT 557 LIMIT 510 -- By Partition Key, on node1, fetch size 5000
     * 		28: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(1699976006349660742, 'ጬ葲') AND token(pk0, pk1) < token(7137864754153440313, '뢸镝蔥') PER PARTITION LIMIT 54 -- by token range, on node2, fetch size 5000
     * 		29: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) BETWEEN -3315371536788945839 AND -9223372036854775808 PER PARTITION LIMIT 406 LIMIT 921 -- min token range, on node3
     * 		30: UPDATE ks1.tbl USING TIMESTAMP 9 SET v1=({f0: false, f1: 0xc50678, f2: 514}, [00000000-0000-1700-9e00-000000000000]), s0=[['02:53:03.301806358', '22:11:05.490315481', '08:57:26.834747163'], ['03:19:25.855999427', '07:43:08.735244495'], ['17:11:43.296045244', '09:50:10.508194464', '13:26:39.023128174']] WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲' AND  ck0 = -1933986024815804926 -- on node1
     * 		31: SELECT * FROM ks1.tbl WHERE v1 = ({f0: false, f1: 0xc50678, f2: 514}, [00000000-0000-1700-9e00-000000000000]) PER PARTITION LIMIT 23 ALLOW FILTERING -- v1 frozen<tuple<frozen<"tOGqK49dTH9n5nZhCHxpmKOPKYO8w4I04vHtYY41ys">, vector<timeuuid, 1>>>, on node1, fetch size 1000
     * 		32: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) BETWEEN token(1699976006349660742, 'ጬ葲') AND token(7137864754153440313, '뢸镝蔥') PER PARTITION LIMIT 297 LIMIT 954 -- by token range, on node1
     * 		33: nodetool flush ks1 tbl
     * 		34: SELECT * FROM ks1.tbl WHERE pk0 = 7137864754153440313 PER PARTITION LIMIT 21 LIMIT 914 ALLOW FILTERING -- pk0 bigint, on node3, fetch size 1
     *
     * Caused by: java.lang.AssertionError: Unexpected results for query: SELECT * FROM ks1.tbl WHERE pk0 = 7137864754153440313 PER PARTITION LIMIT 21 LIMIT 914 ALLOW FILTERING
     * Caused by: java.lang.AssertionError: No rows returned
     * Expected:
     * pk0                 | pk1     | ck0                 | s0                                                                   | v0                                 | v1  
     * 7137864754153440313 | '뢸镝蔥' | 7732824726196172505 | [['11:13:31.615781929', '02:03:35.298191424', '21:32:35.861361643']] | 0x0000000000004d00af00000000000000 | null
     */
    @Test
    public void test3448511221048049990()
    {
        String keyspace = "test3448511221048049990";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"tOGqK49dTH9n5nZhCHxpmKOPKYO8w4I04vHtYY41ys\" (f0 boolean, f1 blob, f2 smallint)", keyspace));
        
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl3448511221048049990 (pk0 bigint, pk1 text, ck0 bigint, s0 frozen<list<frozen<list<time>>>> static, " +
                                          "v0 'org.apache.cassandra.db.marshal.LexicalUUIDType', v1 frozen<tuple<frozen<\"tOGqK49dTH9n5nZhCHxpmKOPKYO8w4I04vHtYY41ys\">, vector<timeuuid, 1>>>, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'", keyspace));

        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl3448511221048049990 USING TIMESTAMP 2 SET s0=[['03:28:16.047802044']] WHERE  pk0 = 7137864754153440313 AND  pk1 = '뢸镝蔥'", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl3448511221048049990 (pk0, pk1, ck0, v0) VALUES (7137864754153440313, '뢸镝蔥', 7732824726196172505, 0x0000000000004d00af00000000000000) USING TIMESTAMP 3", keyspace));

        cluster.get(2).executeInternal(withKeyspace("UPDATE %s.tbl3448511221048049990 USING TIMESTAMP 5 SET s0=[['01:28:35.208066780', '05:25:43.184564123'], ['16:14:58.464860367', '13:59:53.463983006', '10:32:10.674489767']] WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲'", keyspace));
        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl3448511221048049990 (pk0, pk1, s0) VALUES (7137864754153440313, '뢸镝蔥', [['11:13:31.615781929', '02:03:35.298191424', '21:32:35.861361643']]) USING TIMESTAMP 6", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl3448511221048049990 WHERE token(pk0, pk1) BETWEEN token(1699976006349660742, 'ጬ葲') AND token(7137864754153440313, '뢸镝蔥') PER PARTITION LIMIT 297 LIMIT 954", keyspace);
        cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);

        select = withKeyspace("SELECT pk0, pk1, ck0 FROM %s.tbl3448511221048049990 WHERE pk0 = 7137864754153440313 PER PARTITION LIMIT 21 LIMIT 914 ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        // pk0                 | pk1       | ck0                 | s0                                                                   | v0                                 | v1  
        // 7137864754153440313 | '뢸镝蔥' | 7732824726196172505 | [['11:13:31.615781929', '02:03:35.298191424', '21:32:35.861361643']] | 0x0000000000004d00af00000000000000 | null
        assertRows(pagingResult, row(7137864754153440313L, "뢸镝蔥", 7732824726196172505L));
    }

    /*
     * accord.utils.Property$PropertyError: Property error detected:
     * Seed = 3448512096918920638
     * Examples = 1
     * Pure = true
     * Error: Unexpected results for query: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832
     * Steps: 400
     * Values:
     * 	State: 
     * 		Setup:
     * 		Config:
     * 		sstable:
     * 			selected_format: big
     * 		CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';
     * 		CREATE TYPE IF NOT EXISTS ks1."6iiPTW_Oe1eyqpNyLtoSbn" (
     * 		    f0 smallint,
     * 		    f1 uuid
     * 		);;
     * 		CREATE TYPE IF NOT EXISTS ks1."tjQi_gfccLmvemLRbkg" (
     * 		    f0 uuid
     * 		);;
     * 		CREATE TABLE ks1.tbl (
     * 		    pk0 smallint,
     * 		    pk1 double,
     * 		    ck0 int,
     * 		    s0 text static,
     * 		    s1 map<frozen<map<time, double>>, bigint> static,
     * 		    v0 frozen<map<timestamp, timeuuid>>,
     * 		    v1 frozen<set<uuid>>,
     * 		    v2 uuid,
     * 		    v3 frozen<tuple<vector<date, 1>, frozen<"6iiPTW_Oe1eyqpNyLtoSbn">, frozen<"tjQi_gfccLmvemLRbkg">>>,
     * 		    v4 smallint,
     * 		    PRIMARY KEY ((pk0, pk1), ck0)
     * 		) WITH CLUSTERING ORDER BY (ck0 ASC)
     * 		    AND read_repair = 'NONE'
     * 		    AND transactional_mode = 'off'
     * 		    AND transactional_migration_from = 'none'
     * 	History:
     * 		1: DELETE s1 FROM ks1.tbl USING TIMESTAMP 1 WHERE  pk0 = 4217 AND  pk1 = -2.2644046491088394E265 -- on node2
     * 		2: INSERT INTO ks1.tbl (pk0, pk1, s1) VALUES (-16150, 1.0086497658456055E-263, {{'07:58:45.097000261': -2.1560404491129945E225}: 588520316827010420}) USING TIMESTAMP 2 -- on node2
     * 		3: SELECT * FROM ks1.tbl WHERE pk0 = -16150 AND pk1 = 1.0086497658456055E-263 PER PARTITION LIMIT 156 LIMIT 938 -- By Partition Key, on node1, fetch size 5000
     * 		4: SELECT writetime(s0), writetime(s1), writetime(v0), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ks1.tbl
     * 		5: SELECT writetime(s0), writetime(s1), writetime(v0), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ks1.tbl
     * 		6: SELECT * FROM ks1.tbl PER PARTITION LIMIT 996 LIMIT 592 -- full table scan, on node1
     * 		7: SELECT * FROM ks1.tbl PER PARTITION LIMIT 785 LIMIT 299 -- full table scan, on node3
     * 		8: SELECT * FROM ks1.tbl WHERE pk0 = -16150 AND pk1 = 1.0086497658456055E-263 PER PARTITION LIMIT 879 LIMIT 770 -- By Partition Key, on node1, fetch size 100
     * 		9: SELECT writetime(s0), writetime(s1), writetime(v0), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ks1.tbl
     * 		10: SELECT * FROM ks1.tbl WHERE pk0 = -16150 AND pk1 = 1.0086497658456055E-263 PER PARTITION LIMIT 125 LIMIT 406 -- By Partition Key, on node1, fetch size 1
     * 		11: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) = token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 999 LIMIT 939 -- by token, on node3
     * 		12: SELECT writetime(s0), writetime(s1), writetime(v0), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ks1.tbl
     * 		13: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > -9223372036854775808 AND token(pk0, pk1) < -3253266623840194343 PER PARTITION LIMIT 362 LIMIT 270 -- min token range, on node1, fetch size 1
     * 		14: INSERT INTO ks1.tbl (pk0, pk1, ck0, s0, s1, v0) VALUES (4217, -2.2644046491088394E265, -2077196678, '᱔惔겎꣘', null, {'1972-11-15T21:50:31.510Z': 00000000-0000-1100-aa00-000000000000, '1973-10-01T03:02:11.345Z': 00000000-0000-1900-b500-000000000000, '2053-09-18T06:21:05.430Z': 00000000-0000-1900-a100-000000000000}) USING TIMESTAMP 3 -- on node3
     * 		15: SELECT writetime(s0), writetime(s1), writetime(v0), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ks1.tbl
     * 		16: SELECT writetime(s0), writetime(s1), writetime(v0), writetime(v1), writetime(v2), writetime(v3), writetime(v4) FROM ks1.tbl
     * 		17: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) BETWEEN -3253266623840194343 AND -9223372036854775808 PER PARTITION LIMIT 443 LIMIT 895 -- min token range, on node2, fetch size 1
     * 		18: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) <= token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 747 LIMIT 23 -- by token range, on node3, fetch size 1
     * 		19: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) >= -9223372036854775808 AND token(pk0, pk1) < -3253266623840194343 PER PARTITION LIMIT 995 LIMIT 950 -- min token range, on node1, fetch size 5000
     * 		20: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832 -- by token range, on node3, fetch size 10
     *
     * Caused by: java.lang.AssertionError: Unexpected results for query: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832
     * Caused by: java.lang.AssertionError: Unexpected rows found:
     * pk0  | pk1                     | ck0         | s0     | s1   | v0                                                                                                                                                                                                     | v1   | v2   | v3   | v4  
     * 4217 | -2.2644046491088394E265 | -2077196678 | '᱔惔겎꣘' | null | {'1972-11-15T21:50:31.510Z': 00000000-0000-1100-aa00-000000000000, '1973-10-01T03:02:11.345Z': 00000000-0000-1900-b500-000000000000, '2053-09-18T06:21:05.430Z': 00000000-0000-1900-a100-000000000000} | null | null | null | null
     *
     * Expected:
     * pk0 | pk1 | ck0 | s0 | s1 | v0 | v1 | v2 | v3 | v4
     */
    @Test
    public void test3448512096918920638()
    {
        String keyspace = "test3448512096918920638";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"6iiPTW_Oe1eyqpNyLtoSbn\" (f0 smallint, f1 uuid)", keyspace));
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"tjQi_gfccLmvemLRbkg\" (f0 uuid)", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl3448512096918920638 (pk0 smallint, pk1 double, ck0 int, s0 text static, s1 map<frozen<map<time, double>>, bigint> static, " +
                                          "v0 frozen<map<timestamp, timeuuid>>, v1 frozen<set<uuid>>, v2 uuid, v3 frozen<tuple<vector<date, 1>, frozen<\"6iiPTW_Oe1eyqpNyLtoSbn\">, " +
                                          "frozen<\"tjQi_gfccLmvemLRbkg\">>>, v4 smallint, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'", keyspace));

        cluster.get(2).executeInternal(withKeyspace("DELETE s1 FROM %s.tbl3448512096918920638 USING TIMESTAMP 1 WHERE pk0 = 4217 AND  pk1 = -2.2644046491088394E265", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl3448512096918920638 (pk0, pk1, s1) VALUES (-16150, 1.0086497658456055E-263, {{'07:58:45.097000261': -2.1560404491129945E225}: 588520316827010420}) USING TIMESTAMP 2", keyspace));
        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl3448512096918920638 (pk0, pk1, ck0, s0, s1, v0) VALUES (4217, -2.2644046491088394E265, -2077196678, '᱔惔겎꣘', null, {'1972-11-15T21:50:31.510Z': 00000000-0000-1100-aa00-000000000000, '1973-10-01T03:02:11.345Z': 00000000-0000-1900-b500-000000000000, '2053-09-18T06:21:05.430Z': 00000000-0000-1900-a100-000000000000}) USING TIMESTAMP 3", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl3448512096918920638 WHERE token(pk0, pk1) >= -9223372036854775808 AND token(pk0, pk1) < -3253266623840194343 PER PARTITION LIMIT 995 LIMIT 950", keyspace);
        cluster.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 5000);

        // This seems to fail only sporadically...
        select = withKeyspace("SELECT * FROM %s.tbl3448512096918920638 WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 10);
        assertRows(pagingResult);
    }

    /*
     * Seed = 3448154736661599106
     * Examples = 1
     * Pure = true
     * Error: An unexpected error occurred server side on /127.0.0.2:9042: java.lang.IllegalStateException: Multiple partitions received for DecoratedKey(2680073734780247800, 000253ed0000100000000000004100ba0000000000000000)
     * Steps: 400
     * Values:
     * 	State: 
     * 		Setup:
     * 		Config:
     * 		sstable:
     * 			selected_format: bti
     * 		CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';
     * 		CREATE TABLE ks1.tbl (
     * 		    pk0 smallint,
     * 		    pk1 uuid,
     * 		    ck0 'org.apache.cassandra.db.marshal.LexicalUUIDType',
     * 		    ck1 timeuuid,
     * 		    v0 frozen<set<vector<ascii, 2>>>,
     * 		    v1 vector<vector<inet, 2>, 1>,
     * 		    PRIMARY KEY ((pk0, pk1), ck0, ck1)
     * 		) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC)
     * 		    AND read_repair = 'NONE'
     * 		    AND transactional_mode = 'off'
     * 		    AND transactional_migration_from = 'none'
     * 	History:
     * 		1: SELECT * FROM ks1.tbl -- full table scan, on node1
     * 		2: INSERT INTO ks1.tbl (pk0, pk1, ck0, ck1, v0, v1) VALUES (24199, 00000000-0000-4900-9c00-000000000000, 0x0000000000001800b700000000000000, 00000000-0000-1000-8f00-000000000000, {['\u0015\u001AE', '@V\u0002J:hx\u0011']}, [['34.60.146.80', 'a11e:4e06:c09f:8a8:f4f2:ba6f:683d:5e6e']]) USING TIMESTAMP 1 -- on node2
     * 		3: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) < token(24199, 00000000-0000-4900-9c00-000000000000) PER PARTITION LIMIT 83 -- by token range, on node1, fetch size 1
     *      4: SELECT * FROM ks1.tbl WHERE pk0 = 24199 AND pk1 = 00000000-0000-4900-9c00-000000000000 AND ck0 = 0x0000000000001800b700000000000000 AND ck1 = 00000000-0000-1000-8f00-000000000000 -- By Primary Key, on node2
     * 		5: SELECT * FROM ks1.tbl LIMIT 806 -- full table scan, on node3, fetch size 1
     * 		6: DELETE FROM ks1.tbl USING TIMESTAMP 2 WHERE  pk0 = -16322 AND  pk1 = 00000000-0000-4400-ba00-000000000000 -- on node3
     * 		7: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) <= token(24199, 00000000-0000-4900-9c00-000000000000) PER PARTITION LIMIT 630 -- by token range, on node2, fetch size 10
     * 		8: SELECT * FROM ks1.tbl WHERE v0 <= {['\u0015\u001AE', '@V\u0002J:hx\u0011']} ALLOW FILTERING -- v0 frozen<set<vector<ascii, 2>>>, on node2, fetch size 5000
     * 		9: SELECT * FROM ks1.tbl WHERE pk1 <= 00000000-0000-4900-9c00-000000000000 ALLOW FILTERING -- pk1 uuid, on node2, fetch size 1
     * 		10: SELECT * FROM ks1.tbl WHERE pk1 > 00000000-0000-4900-9c00-000000000000 PER PARTITION LIMIT 796 ALLOW FILTERING -- pk1 uuid, on node3, fetch size 10
     * 		11: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) < token(24199, 00000000-0000-4900-9c00-000000000000) PER PARTITION LIMIT 45 LIMIT 699 -- by token range, on node3
     * 		12: UPDATE ks1.tbl USING TIMESTAMP 3 SET v0={['g\u0008\u0009"u', '\u0011)\u0013'], ['zOA&', '\u00019']}, v1=[['56.79.104.226', '106.255.46.196']] WHERE  pk0 = 24199 AND  pk1 = 00000000-0000-4900-9c00-000000000000 AND  ck0 IN (0x00000000000015008100000000000000) AND  ck1 = 00000000-0000-1b00-bd00-000000000000 -- on node3
     * 		13: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) > token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) <= token(24199, 00000000-0000-4900-9c00-000000000000) PER PARTITION LIMIT 196 LIMIT 868 -- by token range, on node3
     * 		14: SELECT * FROM ks1.tbl WHERE ck1 > 00000000-0000-1b00-bd00-000000000000 PER PARTITION LIMIT 361 ALLOW FILTERING -- ck1 timeuuid (reversed), on node2, fetch size 1
     * 		15: INSERT INTO ks1.tbl (pk0, pk1, ck0, ck1, v0, v1) VALUES (21485, 00000000-0000-4100-ba00-000000000000, 0x0000000000004c00a900000000000000, 00000000-0000-1200-b700-000000000000, {['8[y', 'J}T,8'], ['\LPG\u0012\u0015? Q', '\u000DB?[)']}, [['5d78:5bc2:d651:9c78:91e9:a1e6:7247:73c9', '222.212.186.106']]) USING TIMESTAMP 4 -- on node1
     * 		16: SELECT * FROM ks1.tbl WHERE token(pk0, pk1) >= token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) <= token(21485, 00000000-0000-4100-ba00-000000000000) PER PARTITION LIMIT 139 LIMIT 587 -- by token range, on node2, fetch size 100
     */
    @Test
    public void test3448154736661599106()
    {
        String keyspace = "test3448154736661599106";
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", keyspace));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl3448154736661599106 (pk0 smallint, pk1 uuid, ck0 'org.apache.cassandra.db.marshal.LexicalUUIDType', ck1 timeuuid, v0 frozen<set<vector<ascii, 2>>>, v1 vector<vector<inet, 2>, 1>, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC) AND read_repair = 'NONE'", keyspace));

        cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl3448154736661599106", keyspace), ConsistencyLevel.ALL);
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl3448154736661599106 (pk0, pk1, ck0, ck1, v0, v1) VALUES (24199, 00000000-0000-4900-9c00-000000000000, 0x0000000000001800b700000000000000, 00000000-0000-1000-8f00-000000000000, {['\\u0015\\u001AE', '@V\\u0002J:hx\\u0011']}, [['34.60.146.80', 'a11e:4e06:c09f:8a8:f4f2:ba6f:683d:5e6e']]) USING TIMESTAMP 1", keyspace));

        cluster.get(3).executeInternal(withKeyspace("DELETE FROM %s.tbl3448154736661599106 USING TIMESTAMP 2 WHERE  pk0 = -16322 AND  pk1 = 00000000-0000-4400-ba00-000000000000", keyspace));
        cluster.get(3).executeInternal(withKeyspace("UPDATE %s.tbl3448154736661599106 USING TIMESTAMP 3 SET v0={['g\\u0008\\u0009\"u', '\\u0011)\\u0013'], ['zOA&', '\\u00019']}, v1=[['56.79.104.226', '106.255.46.196']] WHERE  pk0 = 24199 AND  pk1 = 00000000-0000-4900-9c00-000000000000 AND  ck0 IN (0x00000000000015008100000000000000) AND  ck1 = 00000000-0000-1b00-bd00-000000000000", keyspace));

        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl3448154736661599106 (pk0, pk1, ck0, ck1, v0, v1) VALUES (21485, 00000000-0000-4100-ba00-000000000000, 0x0000000000004c00a900000000000000, 00000000-0000-1200-b700-000000000000, {['8[y', 'J}T,8'], ['\\LPG\\u0012\\u0015? Q', '\\u000DB?[)']}, [['5d78:5bc2:d651:9c78:91e9:a1e6:7247:73c9', '222.212.186.106']]) USING TIMESTAMP 4", keyspace));

        String select = withKeyspace("SELECT pk0 FROM %s.tbl3448154736661599106 WHERE token(pk0, pk1) >= token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) <= token(21485, 00000000-0000-4100-ba00-000000000000) PER PARTITION LIMIT 139 LIMIT 587", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 100);
        assertRows(pagingResult, row((short) 24199), row((short) 24199), row((short) 21485));
    }

    public static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
}
