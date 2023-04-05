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

package org.apache.cassandra.distributed.test;

import java.util.Date;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class GroupByTest extends TestBaseImpl
{
    @Test
    public void groupByWithDeletesAndSrpOnPartitions() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("user_defined_functions_enabled", "true")).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, PRIMARY KEY (pk, ck))"));
            initFunctions(cluster);
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (1, '1') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (2, '2') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='0'"));

            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '0') USING TIMESTAMP 0"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=1 AND ck='1'"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=2 AND ck='2'"));

            for (String limitClause : new String[]{ "", "LIMIT 1", "LIMIT 10", "PER PARTITION LIMIT 1", "PER PARTITION LIMIT 10" })
            {
                String query = withKeyspace("SELECT concat(ck) FROM %s.tbl GROUP BY pk " + limitClause);
                for (int i = 1; i <= 4; i++)
                {
                    Iterator<Object[]> rows = cluster.coordinator(2).executeWithPaging(query, ConsistencyLevel.ALL, i);
                    assertRows(Iterators.toArray(rows, Object[].class));
                }
            }
        }
    }

    @Test
    public void groupByWithDeletesAndSrpOnRows() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("user_defined_functions_enabled", "true")).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, PRIMARY KEY (pk, ck))"));
            initFunctions(cluster);
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '1') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '2') USING TIMESTAMP 0"));
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='0'"));

            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, '0') USING TIMESTAMP 0"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='1'"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck='2'"));

            for (String limitClause : new String[]{ "", "LIMIT 1", "LIMIT 10", "PER PARTITION LIMIT 1", "PER PARTITION LIMIT 10" })
            {
                String query = withKeyspace("SELECT concat(ck) FROM %s.tbl GROUP BY pk " + limitClause);
                for (int i = 1; i <= 4; i++)
                {
                    Iterator<Object[]> rows = cluster.coordinator(2).executeWithPaging(query, ConsistencyLevel.ALL, i);
                    assertRows(Iterators.toArray(rows, Object[].class));
                }
            }
        }
    }

    @Test
    public void testGroupByWithAggregatesAndPaging() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("user_defined_functions_enabled", "true")).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 text, v2 text, v3 text, primary key (pk, ck))"));
            initFunctions(cluster);

            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v1, v2, v3) values (1,1,'1','1','1')"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v1, v2, v3) values (1,2,'2','2','2')"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v1, v2, v3) values (1,3,'3','3','3')"), ConsistencyLevel.ALL);

            for (int i = 1; i <= 4; i++)
            {
                assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace("select concat(v1), concat(v2), concat(v3) from %s.tbl where pk = 1 group by pk"),
                                                                    ConsistencyLevel.ALL, i),
                           row("_ 1 2 3", "_ 1 2 3", "_ 1 2 3"));

                assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace("select concat(v1), concat(v2), concat(v3) from %s.tbl where pk = 1 group by pk limit 1"),
                                                                    ConsistencyLevel.ALL, i),
                           row("_ 1 2 3", "_ 1 2 3", "_ 1 2 3"));

                assertRows(cluster.coordinator(1).executeWithPaging(withKeyspace("select * from %s.tbl where pk = 1 group by pk"),
                                                                    ConsistencyLevel.ALL, i),
                           row(1, 1, "1", "1", "1"));
            }
        }
    }

    @Test
    public void testGroupWithDeletesAndPaging() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig(cfg -> cfg.with(Feature.GOSSIP, NETWORK, NATIVE_PROTOCOL)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, PRIMARY KEY (pk, ck))"));
            ICoordinator coordinator = cluster.coordinator(1);
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (0, 0)"), ConsistencyLevel.ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.tbl (pk, ck) VALUES (1, 1)"), ConsistencyLevel.ALL);

            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=0 AND ck=0"));
            cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk=1 AND ck=1"));
            String query = withKeyspace("SELECT * FROM %s.tbl GROUP BY pk");
            Iterator<Object[]> rows = coordinator.executeWithPaging(query, ConsistencyLevel.ALL, 1);
            assertRows(Iterators.toArray(rows, Object[].class));

            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session session = c.connect())
            {
                SimpleStatement stmt = new SimpleStatement(withKeyspace("select * from %s.tbl where pk = 1 group by pk"));
                stmt.setFetchSize(1);
                Iterator<Row> rs = session.execute(stmt).iterator();
                Assert.assertFalse(rs.hasNext());
            }
        }
    }

    @Test
    public void testGroupByTimeRangesWithTimestampType() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.testWithTimestamp (pk int, time timestamp, v int, primary key (pk, time))"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:14:00 UTC', 3)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:15:00 UTC', 4)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:21:00 UTC', 5)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:22:00 UTC', 6)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:26:00 UTC', 7)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (1, '2016-09-27 16:26:20 UTC', 8)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (2, '2016-09-27 16:26:20 UTC', 10)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTimestamp (pk, time, v) VALUES (2, '2016-09-27 16:30:00 UTC', 11)"), ConsistencyLevel.QUORUM);

            for (int pageSize : new int[] {2, 3, 4, 5, 7, 10})
            {
                for (String startingTime : new String[] {"", ", '2016-09-27 UTC'"} )
                {
                    String stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp GROUP BY pk, floor(time, 5m" + startingTime + ")";
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L),
                               row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1L),
                               row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2L),
                               row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2L),
                               row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1L),
                               row(2, toTimestamp("2016-09-27 16:30:00 UTC"), 11, 11, 1L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp GROUP BY pk, floor(time, 5m" + startingTime + ") LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L),
                               row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp GROUP BY pk, floor(time, 5m" + startingTime + ") PER PARTITION LIMIT 1";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L),
                               row(2, toTimestamp("2016-09-27 16:25:00 UTC"), 10, 10, 1L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2L),
                               row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2L),
                               row(1, toTimestamp("2016-09-27 16:15:00 UTC"), 4, 4, 1L),
                               row(1, toTimestamp("2016-09-27 16:10:00 UTC"), 1, 3, 3L));

                    stmt = "SELECT pk, floor(time, 5m" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithTimestamp WHERE pk = 1 GROUP BY pk, floor(time, 5m" + startingTime + ") ORDER BY time DESC LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toTimestamp("2016-09-27 16:25:00 UTC"), 7, 8, 2L),
                               row(1, toTimestamp("2016-09-27 16:20:00 UTC"), 5, 6, 2L));
                }
            }
        }
    }

    @Test
    public void testGroupByTimeRangesWithDateType() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.testWithDate (pk int, time date, v int, primary key (pk, time))"));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-27', 1)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-28', 2)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-29', 3)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-09-30', 4)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-10-01', 5)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-10-04', 6)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-10-20', 7)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (1, '2016-11-27', 8)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (2, '2016-11-01', 10)"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithDate (pk, time, v) VALUES (2, '2016-11-02', 11)"), ConsistencyLevel.QUORUM);

            for (int pageSize : new int[] {2, 3, 4, 5, 7, 10})
            {
                for (String startingTime : new String[] {"", ", '2016-06-01'"} )
                {

                    String stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate GROUP BY pk, floor(time, 1mo" + startingTime + ")";
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L),
                               row(1, toLocalDate("2016-11-01"), 8, 8, 1L),
                               row(2, toLocalDate("2016-11-01"), 10, 11, 2L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate GROUP BY pk, floor(time, 1mo" + startingTime + ") LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate GROUP BY pk, floor(time, 1mo" + startingTime + ") PER PARTITION LIMIT 1";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L),
                               row(2, toLocalDate("2016-11-01"), 10, 11, 2L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-11-01"), 8, 8, 1L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L),
                               row(1, toLocalDate("2016-09-01"), 1, 4, 4L));

                    stmt = "SELECT pk, floor(time, 1mo" + startingTime + "), min(v), max(v), count(v) FROM %s.testWithDate WHERE pk = 1 GROUP BY pk, floor(time, 1mo" + startingTime + ") ORDER BY time DESC LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-11-01"), 8, 8, 1L),
                               row(1, toLocalDate("2016-10-01"), 5, 7, 3L));
                }
            }
        }
    }

        @Test
        public void testGroupByTimeRangesWithTimeType() throws Throwable
        {
            try (Cluster cluster = init(builder().withNodes(3).start()))
            {

                cluster.schemaChange(withKeyspace("CREATE TABLE %s.testWithTime (pk int, date date, time time, v int, primary key (pk, date, time))"));

                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:10:00', 1)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:12:00', 2)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:14:00', 3)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:15:00', 4)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:21:00', 5)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:22:00', 6)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:26:00', 7)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-27', '16:26:20', 8)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-28', '16:26:20', 9)"), ConsistencyLevel.QUORUM);
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.testWithTime (pk, date, time, v) VALUES (1, '2016-09-28', '16:26:30', 10)"), ConsistencyLevel.QUORUM);

                for (int pageSize : new int[] {2, 3, 4, 5, 7, 10})
                {

                    String stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime GROUP BY pk, date, floor(time, 5m)";
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2L),
                               row(1, toLocalDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2L));

                    stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime GROUP BY pk, date, floor(time, 5m) LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1L));

                    stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:20:00"), 5, 6, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:15:00"), 4, 4, 1L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:10:00"), 1, 3, 3L));

                    stmt = "SELECT pk, date, floor(time, 5m), min(v), max(v), count(v) FROM %s.testWithTime WHERE pk = 1 GROUP BY pk, date, floor(time, 5m) ORDER BY date DESC, time DESC LIMIT 2";
                    pagingRows = cluster.coordinator(1).executeWithPaging(withKeyspace(stmt), QUORUM, pageSize);
                    assertRows(pagingRows, 
                               row(1, toLocalDate("2016-09-28"), toTime("16:25:00"), 9, 10, 2L),
                               row(1, toLocalDate("2016-09-27"), toTime("16:25:00"), 7, 8, 2L));
                }
            }
        }

    private static void initFunctions(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.concat_strings_fn(a text, b text) " +
                                          "RETURNS NULL ON NULL INPUT " +
                                          "RETURNS text " +
                                          "LANGUAGE java " +
                                          "AS 'return a + \" \" + b;'"));

        cluster.schemaChange(withKeyspace("CREATE AGGREGATE %s.concat(text)" +
                                          " SFUNC concat_strings_fn" +
                                          " STYPE text" +
                                          " INITCOND '_'"));
    }

    private static Date toTimestamp(String timestampAsString)
    {
        return new Date(TimestampSerializer.dateStringToTimestamp(timestampAsString));
    }

    private static int toLocalDate(String dateAsString)
    {
        return SimpleDateSerializer.dateStringToDays(dateAsString) ;
    }

    private static long toTime(String timeAsString)
    {
        return TimeSerializer.timeStringToLong(timeAsString) ;
    }

}