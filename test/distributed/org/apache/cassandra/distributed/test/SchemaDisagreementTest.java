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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.fail;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class SchemaDisagreementTest extends TestBaseImpl
{
    /**
     * If a node receives a mutation for a column it's not aware of, it should fail, since it can't write the data.
     */
    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD v2 int"), 1);

            try
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)"), ALL);
                fail("Should have failed because of schema disagreement.");
            }
            catch (Exception e)
            {
                // for some reason, we get weird errors when trying to check class directly
                // I suppose it has to do with some classloader manipulation going on
                Assert.assertTrue(e.getClass().toString().contains("WriteFailureException"));
                // we may see 1 or 2 failures in here, because of the fail-fast behavior of AbstractWriteResponseHandler
                Assert.assertTrue(e.getMessage().contains("INCOMPATIBLE_SCHEMA from ") &&
                                  (e.getMessage().contains("/127.0.0.2") || e.getMessage().contains("/127.0.0.3")));
            }
        }
    }

    /**
     * If a node receives a mutation for a column it knows has been dropped, the write should succeed.
     */
    @Test
    public void writeWithSchemaDisagreement2() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (1, 1, 1, 1)"));
            cluster.forEach((instance) -> instance.flush(KEYSPACE));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl DROP v2"), 1);

            // execute a write including the dropped column where the coordinator is not yet aware of the drop
            // all nodes should process this without error
            cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)"), ALL);
            // and flushing should also be fine
            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            // the results of reads will vary depending on whether the coordinator has seen the schema change
            // note: read repairs will propagate the v2 value to node1, but this is safe and handled correctly
            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl"), ALL),
                       rows(row(1, 1, 1, 1), row(2, 2, 2, 2)));
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl"), ALL),
                       rows(row(1, 1, 1), row(2, 2, 2)));
        }
    }

    /**
     * If a node isn't aware of a column, but receives a mutation without that column, the write should succeed.
     */
    @Test
    public void writeWithInconsequentialSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD v2 int"), 1);

            // this write shouldn't cause any problems because it doesn't write to the new column
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (2, 2, 2)"), ALL);
        }
    }

    /**
     * If a node receives a read for a column it's not aware of, it shouldn't complain, since it won't have any data for
     * that column.
     */
    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD v2 int"), 1);

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1"), ALL),
                       new Object[][]{ new Object[]{ 1, 1, 1, null } });
        }
    }
}
