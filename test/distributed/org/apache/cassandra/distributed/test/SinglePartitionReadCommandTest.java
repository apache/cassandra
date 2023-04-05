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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class SinglePartitionReadCommandTest extends TestBaseImpl
{
    @Test
    public void testNonCompactTableWithOnlyUpdatedColumnOnOneNodeAndColumnDeletionOnTheOther() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
            cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2000 SET v1 = 1, v2 = 2 WHERE pk = 1 AND ck = '1'"));
            cluster.get(1).flush(KEYSPACE);

            cluster.get(2).executeInternal(withKeyspace("DELETE v1 FROM %s.tbl USING TIMESTAMP 3000 WHERE pk=1 AND ck='1'"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row(1, "1", null, 2));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v1 FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row((Integer) null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v2 FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row((Integer) 2));

            cluster.get(2).executeInternal(withKeyspace("DELETE v2 FROM %s.tbl USING TIMESTAMP 4000 WHERE pk=1 AND ck='1'"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v1 FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v2 FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL));
        }
    }

    @Test
    public void testNonCompactTableWithRowOnOneNodeAndColumnDeletionOnTheOther() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v int, PRIMARY KEY (pk, ck))"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, '1', 1) USING TIMESTAMP 1000"));
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2000 SET v = 2 WHERE pk = 1 AND ck = '1'"));
            cluster.get(1).flush(KEYSPACE);

            cluster.get(2).executeInternal(withKeyspace("DELETE v FROM %s.tbl USING TIMESTAMP 3000 WHERE pk=1 AND ck='1'"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row(1, "1", null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row((Integer) null));

        }
    }

    @Test
    public void testCompactTableWithRowOnOneNodeAndColumnDeletionOnTheOther() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, v1, v2) VALUES (1, 1, 1) USING TIMESTAMP 1000"));
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).executeInternal(withKeyspace("DELETE v1 FROM %s.tbl USING TIMESTAMP 3000 WHERE pk = 1"));
            cluster.get(1).flush(KEYSPACE);

            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl(pk, v1) VALUES (1, 2) USING TIMESTAMP 2000"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row(1, null, 1));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v1, v2 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null, 1));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v1 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null));

        }
    }

    @Test
    public void testPartitionWithStaticColumnsOnlyOnOneNodeAndColumnDeletionOnTheOther() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, s1 int static, s2 int static, v int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, s1, s2) VALUES (1, 1, 1) USING TIMESTAMP 1000"));
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2000 SET s1 = 2 WHERE pk = 1"));
            cluster.get(1).flush(KEYSPACE);

            cluster.get(2).executeInternal(withKeyspace("DELETE s1 FROM %s.tbl USING TIMESTAMP 3000 WHERE pk = 1"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row(1, null, null, 1, null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT s1, s2 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null, 1));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT DISTINCT s1, s2 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null, 1));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT ck FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT s1 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT DISTINCT s1 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL),
                       row((Integer) null));

            cluster.get(2).executeInternal(withKeyspace("DELETE s2 FROM %s.tbl USING TIMESTAMP 4000 WHERE pk = 1"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT s1, s2 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT DISTINCT s1, s2 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT ck FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT s1 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT DISTINCT s1 FROM %s.tbl WHERE pk=1"), ConsistencyLevel.ALL));
        }
    }

    @Test
    public void testNonCompactTableWithEmptyRowOnBothNodes() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v int, PRIMARY KEY (pk, ck))"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, '1', 1) USING TIMESTAMP 1000"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            cluster.coordinator(1).execute(withKeyspace("DELETE v FROM %s.tbl USING TIMESTAMP 2000 WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row(1, "1", null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row((Integer) null));
        }
    }

    @Test
    public void testCompactAndNonCompactTableWithRowOnOneNodeAndRowDeletionOnTheOther() throws Throwable
    {
        for (String options : new String[] {"WITH COMPACT STORAGE", ""})
        {
            try (Cluster cluster = init(builder().withNodes(2).start()))
            {
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v int, PRIMARY KEY (pk, ck)) " + options));
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, '1', 1) USING TIMESTAMP 1000"));
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (2, '1', 1) USING TIMESTAMP 1001"));
                cluster.get(1).flush(KEYSPACE);
                cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2000 SET v = 2 WHERE pk = 1 AND ck = '1'"));
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (2, '2', 2) USING TIMESTAMP 3001"));
                cluster.get(1).flush(KEYSPACE);

                cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2001 WHERE pk=2 AND ck='1'"));
                cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 3000 WHERE pk=1 AND ck='1'"));
                cluster.get(2).flush(KEYSPACE);

                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL));
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL));

                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=2 AND ck='1'"), ConsistencyLevel.ALL,
                           row(2, "2", 2)));
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=2 AND ck='1'"), ConsistencyLevel.ALL,
                           row(2)));
            }
        }
    }

    @Test
    public void testNonCompactTableWithRowOnOneNodeMissingAColumnAndColumnDeletionOnTheOther() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck))"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (1, '1', 1, 1) USING TIMESTAMP 1000"));
            cluster.get(1).flush(KEYSPACE);
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, '1', 2) USING TIMESTAMP 2000"));
            cluster.get(1).flush(KEYSPACE);

            cluster.get(2).executeInternal(withKeyspace("DELETE v1 FROM %s.tbl USING TIMESTAMP 3000 WHERE pk=1 AND ck='1'"));
            cluster.get(2).flush(KEYSPACE);

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row(1, "1", null, 1));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v1 FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row((Integer) null));
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v2 FROM %s.tbl WHERE pk=1 AND ck='1'"), ConsistencyLevel.ALL),
                       row(1));
        }
    }

    @Test
    public void testCompactAndNonCompactTableWithRowOnOneNodeAndRangeDeletionOnTheOther() throws Throwable
    {
        for (String options : new String[] {"WITH COMPACT STORAGE", ""})
        {
            try (Cluster cluster = init(builder().withNodes(2).start()))
            {
                cluster.schemaChange(
                        withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v int, PRIMARY KEY (pk, ck)) " + options));
                cluster.get(1).executeInternal(
                        withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, '1', 1) USING TIMESTAMP 1000"));
                cluster.get(1).executeInternal(
                        withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, '2', 2) USING TIMESTAMP 1001"));
                cluster.get(1).executeInternal(
                        withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (2, '1', 1) USING TIMESTAMP 1001"));
                cluster.get(1).flush(KEYSPACE);
                cluster.get(1).executeInternal(
                        withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2000 SET v = 2 WHERE pk = 1 AND ck = '1'"));
                cluster.get(1).executeInternal(
                        withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (2, '2', 2) USING TIMESTAMP 3001"));
                cluster.get(1).flush(KEYSPACE);

                cluster.get(2).executeInternal(
                        withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2001 WHERE pk=2 AND ck >= '1' AND ck < '2'"));
                cluster.get(2).executeInternal(
                        withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 3000 WHERE pk=1 AND ck >= '1'"));
                cluster.get(2).flush(KEYSPACE);

                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"),
                        ConsistencyLevel.ALL));
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='1'"),
                        ConsistencyLevel.ALL));
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='2'"),
                        ConsistencyLevel.ALL, row(1, "2", 2)));
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='2'"),
                        ConsistencyLevel.ALL, row(2)));

                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'"),
                        ConsistencyLevel.ALL, row(2, "2", 2)));
                assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.tbl WHERE pk=1 AND ck='1'"),
                        ConsistencyLevel.ALL, row(2)));
            }
        }
    }
}