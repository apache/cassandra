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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

public class ReadRepairTest extends TestBaseImpl
{
    @Test
    public void emptyRangeTombstones1() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");
            cluster.get(1).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
            cluster.coordinator(2).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 > ? and column1 <= ?",
                                                 ConsistencyLevel.ALL,
                                                 "test", 10, 10);
            cluster.coordinator(2).execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 > ? and column1 <= ?",
                                                 ConsistencyLevel.ALL,
                                                 "test", 11, 11);
            cluster.get(2).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
    }

    @Test
    public void emptyRangeTombstonesFromPaging() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");

            cluster.get(1).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 10 WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO distributed_test_keyspace.tbl (key, column1) VALUES (?, ?) USING TIMESTAMP 30", ConsistencyLevel.ALL, "test", i);

            consume(cluster.coordinator(2).executeWithPaging("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 >= ? and column1 <= ?",
                                           ConsistencyLevel.ALL, 1,
                                           "test", 8, 12));

            consume(cluster.coordinator(2).executeWithPaging("SELECT * FROM distributed_test_keyspace.tbl WHERE key = ? and column1 >= ? and column1 <= ?",
                                                             ConsistencyLevel.ALL, 1,
                                                             "test", 16, 20));
            cluster.get(2).executeInternal("DELETE FROM distributed_test_keyspace.tbl WHERE key=? AND column1>? AND column1<?;",
                                           "test", Integer.MIN_VALUE, Integer.MAX_VALUE);
        }
    }

    private void consume(Iterator<Object[]> it)
    {
        while (it.hasNext())
            it.next();
    }
}
