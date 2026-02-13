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

package org.apache.cassandra.distributed.test.log;

import java.net.UnknownHostException;

import com.google.common.collect.Sets;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation.Result;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.Startup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StartupTest extends TestBaseImpl
{
    private static final Location TEST_LOCATION = new Location("datacenter1", "rack1");

    private static ClusterMetadata createEmptyMetadata()
    {
        Keyspaces keyspaces = Keyspaces.of(DistributedMetadataLogKeyspace.initialMetadata(Sets.newHashSet("datacenter1")));
        DistributedSchema schema = new DistributedSchema(keyspaces);
        return new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, schema);
    }

    private static ClusterMetadata register(String endpoint, NodeVersion version, ClusterMetadata metadata) throws UnknownHostException
    {
        return new Register(
            new NodeAddresses(InetAddressAndPort.getByName(endpoint)),
            TEST_LOCATION,
            version
        ).execute(metadata).success().metadata;
    }

    /**
     * Tests that the Startup transformation rejects downgrading a node to a version
     * that cannot read cluster metadata.
     *
     * Scenario:
     * - Cluster has 1 node running at CURRENT_METADATA_VERSION
     * - commonSerializationVersion = CURRENT_METADATA_VERSION
     * - Node tries to "downgrade" by restarting with a lower version
     * - Should be REJECTED because the lower version cannot read the current metadata
     */
    @Test
    public void testStartupRejectsDowngrade() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).createWithoutStarting())
        {
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                NodeId nodeId = metadata.myNodeId();

                // Try to "downgrade" the node to V3 (simulating restart with older binary)
                NodeVersion downgradedVersion = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V3);
                Startup startup = new Startup(nodeId, metadata.directory.getNodeAddresses(nodeId), downgradedVersion);

                Result result = startup.execute(metadata);

                assertTrue("Startup should be rejected for downgrade to lower serialization version",
                           result.isRejected());
                assertTrue("Rejection message should mention serialization version",
                           result.rejected().reason.contains("serialization version"));
            });
        }
    }

    /**
     * Tests that the Startup transformation allows a node to restart with equal or higher
     * serialization version.
     *
     * Scenario:
     * - Create empty ClusterMetadata with Directory.EMPTY
     * - Register a V3 node (succeeds because directory is empty - first node)
     * - Register a second node to test Startup against
     * - Test Startup with V5 (higher than V3) - should succeed
     * - Test Startup with V3 (equal to V3) - should succeed
     */
    @Test
    public void testStartupAllowsEqualOrHigherSerializationVersion() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).createWithoutStarting())
        {
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    // Register first node with V3 - succeeds because directory is empty
                    NodeVersion v3 = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V3);
                    ClusterMetadata metadataWithV3Node = register("127.0.0.10", v3, createEmptyMetadata());

                    assertEquals("commonSerializationVersion should be V3",
                                 Version.V3, metadataWithV3Node.directory.commonSerializationVersion);

                    // Register a second node to test Startup against
                    ClusterMetadata testMetadata = register("127.0.0.11", v3, metadataWithV3Node);
                    NodeId testNodeId = testMetadata.directory.peerId(InetAddressAndPort.getByName("127.0.0.11"));

                    // Test Startup with V5 (higher than V3) - should succeed
                    NodeVersion v5 = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V5);
                    Startup startupV5 = new Startup(testNodeId, testMetadata.directory.getNodeAddresses(testNodeId), v5);

                    Result resultV5 = startupV5.execute(testMetadata);
                    assertTrue("Startup should succeed for V5 when cluster is at V3",
                               resultV5.isSuccess());

                    // Test Startup with V3 (equal to cluster version) - should succeed
                    Startup startupV3 = new Startup(testNodeId, testMetadata.directory.getNodeAddresses(testNodeId), v3);

                    Result resultV3 = startupV3.execute(testMetadata);
                    assertTrue("Startup should succeed for V3 when cluster is at V3",
                               resultV3.isSuccess());
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
