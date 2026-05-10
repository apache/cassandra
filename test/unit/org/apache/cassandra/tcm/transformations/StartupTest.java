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

package org.apache.cassandra.tcm.transformations;

import java.net.UnknownHostException;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StartupTest
{
    private static final Location LOCATION = new Location("dc", "rack");

    /**
     * Tests that the Startup transformation rejects downgrading a node to a version
     * that cannot read cluster metadata.
     */
    @Test
    public void rejectsDowngrade() throws UnknownHostException
    {
        NodeId nodeId = new NodeId(1);
        NodeAddresses addresses = new NodeAddresses(InetAddressAndPort.getByName("127.0.0.1"));

        Directory directory = Directory.EMPTY
                              .unsafeWithNodeForTesting(nodeId, addresses, LOCATION, NodeVersion.CURRENT)
                              .withNodeState(nodeId, NodeState.JOINED);

        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance)
                                                            .transformer()
                                                            .with(directory)
                                                            .build().metadata;

        assertEquals("commonSerializationVersion should be CURRENT_METADATA_VERSION",
                     NodeVersion.CURRENT_METADATA_VERSION, metadata.directory.commonSerializationVersion);

        // Try to "downgrade" the node to V3 (simulating restart with older binary)
        NodeVersion downgradedVersion = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V3);
        Startup startup = new Startup(nodeId, addresses, downgradedVersion);

        Transformation.Result result = startup.execute(metadata);

        assertTrue("Startup should be rejected for downgrade to lower serialization version", result.isRejected());
        assertEquals(ExceptionCode.INVALID, result.rejected().code);
    }

    /**
     * Tests that the Startup transformation allows a node to restart with equal or higher
     * serialization version.
     */
    @Test
    public void allowsEqualOrHigherSerializationVersion() throws UnknownHostException
    {
        NodeId nodeId = new NodeId(1);
        NodeAddresses addresses = new NodeAddresses(InetAddressAndPort.getByName("127.0.0.1"));
        NodeVersion v3 = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V3);

        Directory directory = Directory.EMPTY
                              .unsafeWithNodeForTesting(nodeId, addresses, LOCATION, v3)
                              .withNodeState(nodeId, NodeState.JOINED);

        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance)
                                                            .transformer()
                                                            .with(directory)
                                                            .build().metadata;

        assertEquals("commonSerializationVersion should be V3", Version.V3, metadata.directory.commonSerializationVersion);

        // Startup with higher version - should succeed
        Startup startupHigher = new Startup(nodeId, addresses, NodeVersion.CURRENT);

        Transformation.Result resultHigher = startupHigher.execute(metadata);
        assertTrue("Startup should succeed for higher serialization version", resultHigher.isSuccess());

        // Startup with equal version - should succeed
        Startup startupEqual = new Startup(nodeId, addresses, v3);

        Transformation.Result resultEqual = startupEqual.execute(metadata);
        assertTrue("Startup should succeed for equal serialization version", resultEqual.isSuccess());
    }
}
