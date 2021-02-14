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

import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class GroupByTest extends TestBaseImpl
{
    @Test
    public void groupByWithDeletesAndSrpOnPartitions() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("enable_user_defined_functions", "true")).start()))
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
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("enable_user_defined_functions", "true")).start()))
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
        try (Cluster cluster = init(builder().withNodes(2).withConfig((cfg) -> cfg.set("enable_user_defined_functions", "true")).start()))
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
}