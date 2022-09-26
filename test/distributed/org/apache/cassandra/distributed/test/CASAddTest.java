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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.AssertionUtils;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class CASAddTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(CASAddTest.class);

    /**
     * The {@code cas_contention_timeout} used during the tests
     */
    private static final long CONTENTION_TIMEOUT = 1000L;

    /**
     * The {@code write_request_timeout} used during the tests
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
    public void testAdditionNotExists() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, a int, b text)");

            // in this context partition/row not existing looks like column not existing, so to simplify the LWT required
            // condition, add a row with null columns so can rely on IF EXISTS
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (1)", ConsistencyLevel.QUORUM);

            // n = n + value where n = null
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET a = a + 1, b = b + 'fail' WHERE pk = 1 IF EXISTS", ConsistencyLevel.QUORUM);
            // the SET should all no-op due to null... so should no-op
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, null, null));

            // this section is testing current limitations... if they start to fail due to the limitations going away... update this test to include those cases
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(batch(
                      "INSERT INTO " + KEYSPACE + ".tbl (pk, a, b) VALUES (1, 0, '') IF NOT EXISTS",
                      "UPDATE " + KEYSPACE + ".tbl SET a = a + 1, b = b + 'success' WHERE pk = 1 IF EXISTS"
                      ), ConsistencyLevel.QUORUM))
                      .is(AssertionUtils.is(InvalidRequestException.class))
                      .hasMessage("Cannot mix IF EXISTS and IF NOT EXISTS conditions for the same row");
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(batch(
                      "INSERT INTO " + KEYSPACE + ".tbl (pk, a, b) VALUES (1, 0, '') IF NOT EXISTS",

                      "UPDATE " + KEYSPACE + ".tbl SET a = a + 1, b = b + 'success' WHERE pk = 1"
                      ), ConsistencyLevel.QUORUM))
                      .is(AssertionUtils.is(InvalidRequestException.class))
                      .hasMessage("Invalid operation (a = a + 1) for non counter column a");

            // since CAS doesn't allow the above cases, manually add the data to unblock...
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, a, b) VALUES (1, 0, '')", ConsistencyLevel.QUORUM);

            // have cas add defaults when missing
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET a = a + 1, b = b + 'success' WHERE pk = 1 IF EXISTS", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL), row(1, 1, "success"));
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
