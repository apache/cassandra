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

package org.apache.cassandra.distributed.test.tcm;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.util.Auth.waitForExistingRoles;
import static org.junit.Assert.assertEquals;

public class TruncationTest extends TestBaseImpl
{
    @Test
    public void truncationTest() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                           .start(), 2))
        {
            final IInvokableInstance node1 = cluster.get(1);
            final IInvokableInstance node2 = cluster.get(2);

            waitForExistingRoles(node1);
            waitForExistingRoles(node2);

            cluster.schemaChange(format("CREATE TABLE %s.cf (k text PRIMARY KEY, c1 text)", KEYSPACE));

            node1.coordinator().execute(format("TRUNCATE %s.cf", KEYSPACE), ConsistencyLevel.ONE);

            Thread.sleep(10000);

            UUID tableId = node1.appliesOnInstance((IIsolatedExecutor.SerializableBiFunction<String, String, UUID>) (ks, tb) -> {
                return ClusterMetadata.current().schema.getKeyspaceMetadata(ks).getTableNullable(tb).id.asUUID();
            }).apply(KEYSPACE, "cf");

            Long node1TruncationTime = getTruncationTime(node1, tableId);
            Long node2TruncationTime = getTruncationTime(node2, tableId);

            assertEquals(node1TruncationTime, node2TruncationTime);
        }
    }

    private long getTruncationTime(IInvokableInstance node, UUID tableId)
    {
        return node.appliesOnInstance((IIsolatedExecutor.SerializableFunction<UUID, Long>) (uuid) -> {
            return SystemKeyspace.getTruncatedAt(TableId.fromUUID(uuid));
        }).apply(tableId);
    }
}
