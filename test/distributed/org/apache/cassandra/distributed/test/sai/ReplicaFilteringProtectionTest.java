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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class ReplicaFilteringProtectionTest extends TestBaseImpl
{
    private static final int REPLICAS = 2;

    @Test
    public void testRFPWithIndexTransformations() throws IOException
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(REPLICAS)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                       .set("commitlog_sync", "batch")).start()))
        {
            String tableName = "sai_rfp";
            String fullTableName = KEYSPACE + '.' + tableName;

            cluster.schemaChange("CREATE TABLE " + fullTableName + " (k int PRIMARY KEY, v text)");
            cluster.schemaChange("CREATE CUSTOM INDEX ON " + fullTableName + "(v) USING 'StorageAttachedIndex' " +
                                 "WITH OPTIONS = { 'case_sensitive' : false}");

            // both nodes have the old value
            cluster.coordinator(1).execute("INSERT INTO " + fullTableName + "(k, v) VALUES (0, 'OLD')", ALL);

            String select = "SELECT * FROM " + fullTableName + " WHERE v = 'old'";
            Object[][] initialRows = cluster.coordinator(1).execute(select, ALL);
            assertRows(initialRows, row(0, "OLD"));

            // only one node gets the new value
            cluster.get(1).executeInternal("UPDATE " + fullTableName + " SET v = 'new' WHERE k = 0");

            // querying by the old value shouldn't return the old surviving row
            SimpleQueryResult oldResult = cluster.coordinator(1).executeWithResult(select, ALL);
            assertRows(oldResult.toObjectArrays());
        }
    }
}
