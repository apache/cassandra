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

public class RegisterTest
{
    private static final Location LOCATION = new Location("dc", "rack");

    /**
     * Tests that registering a new node with a serialization version lower than the cluster's
     * commonSerializationVersion is rejected.
     */
    @Test
    public void rejectsLowerSerializationVersion() throws UnknownHostException
    {
        NodeId existingNode = new NodeId(1);

        Directory directory = Directory.EMPTY
                              .unsafeWithNodeForTesting(existingNode,
                                                        new NodeAddresses(InetAddressAndPort.getByName("127.0.0.1")),
                                                        LOCATION,
                                                        NodeVersion.CURRENT)
                              .withNodeState(existingNode, NodeState.JOINED);

        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance)
                                                            .transformer()
                                                            .with(directory)
                                                            .build().metadata;

        assertEquals("commonSerializationVersion should be CURRENT_METADATA_VERSION", NodeVersion.CURRENT_METADATA_VERSION, metadata.directory.commonSerializationVersion);

        // Try to register a new node with V3 (lower than cluster's current version)
        NodeVersion lowerVersion = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V3);
        Register register = new Register(
            new NodeAddresses(InetAddressAndPort.getByName("127.0.0.2")),
            LOCATION,
            lowerVersion
        );

        Transformation.Result result = register.execute(metadata);

        assertTrue("Registration should be rejected for node with lower serialization version", result.isRejected());
        assertEquals(ExceptionCode.INVALID, result.rejected().code);
    }

    /**
     * Tests that registering nodes with serialization version equal to or higher than
     * the cluster's commonSerializationVersion is allowed.
     */
    @Test
    public void allowsEqualOrHigherSerializationVersion() throws UnknownHostException
    {
        NodeId existingNode = new NodeId(1);
        NodeVersion v3 = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V3);

        Directory directory = Directory.EMPTY
                              .unsafeWithNodeForTesting(existingNode,
                                                        new NodeAddresses(InetAddressAndPort.getByName("127.0.0.1")),
                                                        LOCATION,
                                                        v3)
                              .withNodeState(existingNode, NodeState.JOINED);

        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance)
                                                            .transformer()
                                                            .with(directory)
                                                            .build().metadata;

        assertEquals("commonSerializationVersion should be V3", Version.V3, metadata.directory.commonSerializationVersion);

        // Register a node with higher version - should succeed
        Register registerHigher = new Register(
            new NodeAddresses(InetAddressAndPort.getByName("127.0.0.2")),
            LOCATION,
            NodeVersion.CURRENT
        );

        Transformation.Result resultHigher = registerHigher.execute(metadata);
        assertTrue("Registration should succeed for node with higher serialization version", resultHigher.isSuccess());

        // Register a node with equal version - should succeed
        Register registerEqual = new Register(
            new NodeAddresses(InetAddressAndPort.getByName("127.0.0.3")),
            LOCATION,
            v3
        );

        Transformation.Result resultEqual = registerEqual.execute(metadata);
        assertTrue("Registration should succeed for node with equal serialization version", resultEqual.isSuccess());
    }

    /**
     * Tests that the first node in an empty cluster can register with any version
     * (bypasses version check because directory is empty).
     */
    @Test
    public void allowsAnyVersionForFirstNode() throws UnknownHostException
    {
        ClusterMetadata metadata = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);

        assertTrue("Directory should be empty", metadata.directory.isEmpty());

        // Register first node with V0 - should succeed because directory is empty
        NodeVersion v0 = new NodeVersion(NodeVersion.CURRENT.cassandraVersion, Version.V0);
        Register register = new Register(
            new NodeAddresses(InetAddressAndPort.getByName("127.0.0.1")),
            LOCATION,
            v0
        );

        Transformation.Result result = register.execute(metadata);
        assertTrue("First node registration should succeed with any version", result.isSuccess());
    }
}