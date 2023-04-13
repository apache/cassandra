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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.virtual.ColumnIndexesSystemView;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.assertj.core.util.Streams;

import static org.awaitility.Awaitility.await;

public class SAIUtil
{
    /**
     * Waits until all indexes in the given keyspace become queryable.
     */
    public static void waitForIndexQueryable(Cluster cluster, String keyspace)
    {
        assertGossipEnabled(cluster);
        final List<String> indexes = getIndexes(cluster, keyspace);
        await().atMost(60, TimeUnit.SECONDS)
               .untilAsserted(() -> assertIndexesQueryable(cluster, keyspace, indexes));
    }

    /**
     * Waits until given index becomes queryable.
     */
    public static void waitForIndexQueryable(Cluster cluster, String keyspace, String index)
    {
        assertGossipEnabled(cluster);
        await().atMost(60, TimeUnit.SECONDS)
               .untilAsserted(() -> assertIndexQueryable(cluster, keyspace, index));
    }

    private static void assertGossipEnabled(Cluster cluster)
    {
        cluster.stream().forEach(node -> {
            assert node.config().has(Feature.NETWORK) : "Network not enabled on this cluster";
            assert node.config().has(Feature.GOSSIP) : "Gossip not enabled on this cluster";
        });
    }

    /**
     * Checks if index is known to be queryable, by pulling index state from {{@link SecondaryIndexManager}}.
     * Requires gossip.
     */
    public static void assertIndexQueryable(Cluster cluster, String keyspace, String index)
    {
        assertIndexesQueryable(cluster, keyspace, Collections.singleton(index));
    }

    /**
     * Checks if all indexes are known to be queryable, by pulling index state from local {{@link SecondaryIndexManager}}.
     * Requires gossip.
     */
    private static void assertIndexesQueryable(Cluster cluster, String keyspace, final Iterable<String> indexes)
    {
        IInvokableInstance localNode = cluster.get(1);
        final List<InetAddressAndPort> nodes =
            cluster.stream()
                   .map(node -> nodeAddress(node.broadcastAddress()))
                   .collect(Collectors.toList());

        localNode.runOnInstance(() -> {
            for (String index : indexes)
            {
                for (InetAddressAndPort node : nodes)
                {
                    Index.Status status = IndexStatusManager.instance.getIndexStatus(node, keyspace, index);
                    assert status == Index.Status.BUILD_SUCCEEDED
                        : "Index " + index + " not queryable on node " + node + " (status = " + status + ')';
                }
            }
        });
    }

    private static InetAddressAndPort nodeAddress(InetSocketAddress address)
    {
        return InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
    }

    /**
     * Returns names of the indexes in the keyspace, found on the first node of the cluster.
     */
    public static List<String> getIndexes(Cluster cluster, String keyspace)
    {
        waitForSchemaAgreement(cluster);
        String query = String.format("SELECT index_name FROM system_views.%s WHERE keyspace_name = '%s' ALLOW FILTERING",
                                     ColumnIndexesSystemView.NAME, keyspace);
        SimpleQueryResult result = cluster.get(1).executeInternalWithResult(query);
        return Streams.stream(result)
                      .map(row -> (String) row.get("index_name"))
                      .collect(Collectors.toList());
    }

    public static void waitForSchemaAgreement(Cluster cluster)
    {
        await().atMost(60, TimeUnit.SECONDS)
               .until(() -> schemaAgrees(cluster));
    }

    /**
     * Returns true if schema agrees on all nodes of the cluster
     */
    public static boolean schemaAgrees(Cluster cluster)
    {
        Set<UUID> versions = cluster.stream()
                                    .map(IInstance::schemaVersion)
                                    .collect(Collectors.toSet());
        return versions.size() == 1;
    }
}
