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
package org.apache.cassandra.tools.nodetool.mock;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.LocationInfoMBean;
import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.mockito.ArgumentMatchers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class DescribeClusterMockTest extends AbstractNodetoolMock
{
    @Test
    public void testDescribeCluster() throws Throwable
    {
        StorageServiceMBean ssMock = getMock(STORAGE_SERVICE_MBEAN);
        LocationInfoMBean locationInfoMock = getMock(LOCATION_INFO_MBEAN);
        StorageProxyMBean spMock = getMock(STORAGE_PROXY_MBEAN);
        EndpointSnitchInfoMBean epSnitchInfoMock = getMock(ENDPOINT_SNITCH_INFO_MBEAN);

        when(epSnitchInfoMock.getDatacenter(ArgumentMatchers.any())).thenReturn("datacenter1");

        when(locationInfoMock.getDatacenter()).thenReturn("datacenter1");
        when(locationInfoMock.getRack()).thenReturn("rack1");
        when(locationInfoMock.getNodeProximityName()).thenReturn("dc1-rack1");
        when(locationInfoMock.hasLegacySnitchAdapter()).thenReturn(false);

        when(ssMock.getClusterName()).thenReturn("Test Cluster");
        when(ssMock.getPartitionerName()).thenReturn("org.apache.cassandra.dht.Murmur3Partitioner");
        when(ssMock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(ssMock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        when(ssMock.getJoiningNodesWithPort()).thenReturn(List.of("node1"));
        when(ssMock.getLeavingNodesWithPort()).thenReturn(List.of("node2"));
        when(ssMock.getMovingNodesWithPort()).thenReturn(List.of("node3"));
        when(ssMock.getLiveNodesWithPort()).thenReturn(List.of("node4"));
        when(ssMock.getUnreachableNodesWithPort()).thenReturn(List.of("node5"));
        when(ssMock.getTokenToEndpointWithPortMap()).thenReturn(Map.of("token1", "localhost:7000"));
        when(ssMock.effectiveOwnershipWithPort(keyspace())).thenReturn(Map.of("node7", 1.0F));
        when(ssMock.getKeyspaceReplicationInfo(keyspace())).thenReturn("{'class':'SimpleStrategy', 'replication_factor':1}");
        when(spMock.getSchemaVersionsWithPort()).thenReturn(Map.of("proxy1", List.of("1.0", "2.0")));

        ToolRunner.ToolResult result = invokeNodetool("--print-port", "describecluster");
        result.assertOnCleanExit();

        assertThat(result.getStdout()).contains("cql_test_keyspace -> Replication class: {'class':'SimpleStrategy', 'replication_factor':1}");
        assertThat(result.getStdout()).contains("Partitioner: org.apache.cassandra.dht.Murmur3Partitioner");
    }
}
