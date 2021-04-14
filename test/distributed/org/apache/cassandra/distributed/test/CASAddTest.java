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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CASAddTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(CASAddTest.class);

    /**
     * The {@code cas_contention_timeout_in_ms} used during the tests
     */
    private static final long CONTENTION_TIMEOUT = 1000L;

    /**
     * The {@code write_request_timeout_in_ms} used during the tests
     */
    private static final long REQUEST_TIMEOUT = 1000L;

    @Test
    public void testAddition() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v int)");

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, 1));
            
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = v + 1 WHERE pk = 1 IF v = 2", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, 1));

            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = v + 1 WHERE pk = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, 2));
        }
    }

    @Test
    public void testConcat() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v text)");

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (1, 'foo') IF NOT EXISTS", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, "foo"));

            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = v + 'bar' WHERE pk = 1 IF v = 'foobar'", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, "foo"));

            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = v + 'bar' WHERE pk = 1 IF v = 'foo'", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, "foobar"));
        }
    }

}
