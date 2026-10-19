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

package org.apache.cassandra.distributed.test.jmx;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves that {@code StorageServiceMBean.getReplicaPeers} marshals over a remote JMX connection.
 */
public class ReplicaPeersJmxTest extends TestBaseImpl
{
    private static final String KEYSPACE = "replica_peers_jmx_test";
    private static final String OPERATION = "getReplicaPeers";
    private static final String[] SIGNATURE = { String.class.getName(),
                                                Set.class.getName(),
                                                Set.class.getName(),
                                                Set.class.getName(),
                                                boolean.class.getName(),
                                                String.class.getName() };

    @Test
    public void testGetReplicaPeersOverRemoteJmx() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3).withConfig(c -> c.with(Feature.values())).start();
             JMXConnector connector = JMXUtil.getJmxConnector(cluster.get(1).config()))
        {
            fixDistributedSchemas(cluster);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE +
                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");

            MBeanServerConnection mbsc = connector.getMBeanServerConnection();
            ObjectName objectName = ObjectName.getInstance("org.apache.cassandra.db:type=StorageService");

            // every node, every datacenter, catching up with the CMS first
            Object[] wholeCluster = { null, null, new HashSet<>(List.of(KEYSPACE)), null, true, "JSON" };
            Map<String, Object> tree = parse(mbsc.invoke(objectName, OPERATION, wholeCluster, SIGNATURE));

            assertThat(tree).containsKeys("epoch", "keyspaces", "endpoints", "peers", "catch_up", "cms", "queried_from");
            assertThat(tree.get("catch_up")).isEqualTo("OK");
            assertThat(tree.get("cms")).isEqualTo("INITIALIZED");

            Map<String, Object> endpoints = cast(tree.get("endpoints"));
            Map<String, Object> peers = cast(tree.get("peers"));
            assertThat(peers).hasSize(3);
            assertThat(endpoints.keySet()).containsAll(peers.keySet());

            // At RF=3 in a 3 node cluster every node co-replicates with both others
            for (Object o : peers.values())
            {
                Map<String, Object> sets = cast(o);
                assertThat(countPeers(cast(sets.get("read")))).isEqualTo(2);
                assertThat(countPeers(cast(sets.get("write")))).isEqualTo(2);
            }

            // A single node, and without the CMS round trip
            Object[] singleNode = { "1", null, new HashSet<>(List.of(KEYSPACE)), null, false, "JSON" };
            Map<String, Object> single = parse(mbsc.invoke(objectName, OPERATION, singleNode, SIGNATURE));
            assertThat(cast(single.get("peers"))).hasSize(1);
            assertThat(single.get("catch_up")).isEqualTo("SKIPPED");

            // By default, every replicated keyspace counts, system ones included
            Object[] defaults = { null, null, null, null, true, "JSON" };
            Map<String, Object> withDefaults = parse(mbsc.invoke(objectName, OPERATION, defaults, SIGNATURE));
            assertThat(keyspaces(withDefaults)).contains("system_cluster_metadata", "system_auth", KEYSPACE);

            // And an exclusion list leaves out exactly what it names
            Object[] exclusions = { null, null, null, new HashSet<>(List.of("system_auth")), true, "JSON" };
            Map<String, Object> excluded = parse(mbsc.invoke(objectName, OPERATION, exclusions, SIGNATURE));
            assertThat(keyspaces(excluded)).contains("system_cluster_metadata", KEYSPACE)
                                           .doesNotContain("system_auth");
        }
    }

    @Test
    public void testRejectsInvalidArguments() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(Feature.values())).start();
             JMXConnector connector = JMXUtil.getJmxConnector(cluster.get(1).config()))
        {
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();
            ObjectName objectName = ObjectName.getInstance("org.apache.cassandra.db:type=StorageService");

            Object[] unknownNode = { "127.99.99.99", null, null, null, false, "JSON" };
            assertThatThrownBy(() -> mbsc.invoke(objectName, OPERATION, unknownNode, SIGNATURE));

            Object[] unknownDc = { null, new HashSet<>(List.of("no_such_dc")), null, null, false, "JSON" };
            assertThatThrownBy(() -> mbsc.invoke(objectName, OPERATION, unknownDc, SIGNATURE));

            Object[] nodeAndDc = { "1", new HashSet<>(List.of("datacenter0")), null, null, false, "JSON" };
            assertThatThrownBy(() -> mbsc.invoke(objectName, OPERATION, nodeAndDc, SIGNATURE));
        }
    }

    @Test
    public void testCatchUpFailsWhileMetadataLogIsPaused() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(Feature.values())).start();
             JMXConnector connector = JMXUtil.getJmxConnector(cluster.get(1).config()))
        {
            IInvokableInstance instance = cluster.get(1);
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();
            ObjectName objectName = ObjectName.getInstance("org.apache.cassandra.db:type=StorageService");

            Object[] withCatchUp = { null, null, null, null, true, "JSON" };

            // A paused log still accepts fetched entries, it just does not enact them, so catching up would appear to
            // succeed while answering from stale metadata. It must fail instead.
            instance.runOnInstance(() -> ClusterMetadataService.instance().log().pause());
            try
            {
                assertThatThrownBy(() -> mbsc.invoke(objectName, OPERATION, withCatchUp, SIGNATURE))
                    .hasStackTraceContaining("Metadata log processing is paused");

                // Opting out of the catch-up is how to get an answer, knowingly from local metadata, regardless
                Object[] withoutCatchUp = { null, null, null, null, false, "JSON" };
                Map<String, Object> local = parse(mbsc.invoke(objectName, OPERATION, withoutCatchUp, SIGNATURE));
                assertThat(local.get("catch_up")).isEqualTo("SKIPPED");
            }
            finally
            {
                instance.runOnInstance(() -> ClusterMetadataService.instance().log().unpause());
            }

            Map<String, Object> resumed = parse(mbsc.invoke(objectName, OPERATION, withCatchUp, SIGNATURE));
            assertThat(resumed.get("catch_up")).isEqualTo("OK");
        }
    }

    private static Map<String, Object> parse(Object response) throws Exception
    {
        assertThat(response).isInstanceOf(String.class);
        return JsonUtils.JSON_OBJECT_MAPPER.readValue((String) response, Map.class);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Object value)
    {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<String> keyspaces(Map<String, Object> tree)
    {
        return (List<String>) tree.get("keyspaces");
    }

    private static int countPeers(Map<String, Object> byDatacenter)
    {
        int total = 0;
        for (Object peers : byDatacenter.values())
            total += ((List<?>) peers).size();
        return total;
    }
}
