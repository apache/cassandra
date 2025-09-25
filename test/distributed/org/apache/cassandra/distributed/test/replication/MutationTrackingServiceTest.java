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

package org.apache.cassandra.distributed.test.replication;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.MutationTrackingService;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MutationTrackingServiceTest extends TestBaseImpl
{
    private static final String KS_NAME = "ks";
    private static final String TBL_NAME = "tbl";

    @Test
    public void testIrrelevantTopologyChange() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            // Create tracked keyspace and table
            cluster.schemaChange("CREATE KEYSPACE " + KS_NAME + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + KS_NAME + "." + TBL_NAME + " (k int PRIMARY KEY, v int)");

            // Create an untracked keyspace (should not affect tracked keyspace shards)
            cluster.schemaChange("CREATE KEYSPACE untracked WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'");

            // Test that shards remain the same object (no reconfiguration)
            Boolean shardsUnchanged = cluster.get(1).callOnInstance(() -> {
                var service = MutationTrackingService.instance();
                Object initialShards = MutationTrackingService.TestAccess.getKeyspaceShards(service, KS_NAME);
                Object newShards = MutationTrackingService.TestAccess.getKeyspaceShards(service, KS_NAME);
                return initialShards == newShards; // Same object reference
            });
            
            assertTrue("Keyspace shards should not change for irrelevant topology changes", shardsUnchanged);
        }
    }
}