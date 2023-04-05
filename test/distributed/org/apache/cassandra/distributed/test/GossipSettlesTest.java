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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class GossipSettlesTest extends TestBaseImpl
{
    @Test
    public void testGossipSettles() throws Throwable
    {
        /* Use withSubnet(1) to prove seed provider is set correctly - without the fix to pass a seed provider, this test fails */
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .withSubnet(1)
                                        .start())
        {
            // Verify the 4.0 WithPort versions of status reporting methods match their InetAddress
            // counterparts.  Disable Gossip first to prevent any bump in heartbeats that would
            // invalidate the comparison.  Compare the newer WithPort versions by adding the
            // storage port to IP addresses in keys/values/strings as appropriate.
            cluster.forEach(i -> i.runOnInstance(() -> { Gossiper.instance.stop(); }));
            cluster.get(1).runOnInstance(() -> {

                // First prove that the storage port is added
                Assert.assertEquals("stuff 127.0.0.1:7012 morestuff 127.0.0.2:7012", addStoragePortToIP("stuff 127.0.0.1 morestuff 127.0.0.2"));

                FailureDetector fd = ((FailureDetector) FailureDetector.instance);
                Assert.assertEquals(addStoragePortToInstanceName(fd.getAllEndpointStates(false)),
                                    fd.getAllEndpointStates(true));
                Assert.assertEquals(addPortToKeys(fd.getSimpleStates()), fd.getSimpleStatesWithPort());

                StorageProxy sp = StorageProxy.instance;
                Assert.assertEquals(addPortToSchemaVersions(sp.getSchemaVersions()), sp.getSchemaVersionsWithPort());

                StorageService ss = StorageService.instance;
                Assert.assertEquals(addPortToValues(ss.getTokenToEndpointMap()), ss.getTokenToEndpointWithPortMap());
                Assert.assertEquals(addPortToKeys(ss.getEndpointToHostId()), ss.getEndpointWithPortToHostId());
                Assert.assertEquals(addPortToValues(ss.getHostIdToEndpoint()), ss.getHostIdToEndpointWithPort());
                Assert.assertEquals(addPortToKeys(ss.getLoadMap()), ss.getLoadMapWithPort());
                Assert.assertEquals(addPortToList(ss.getLiveNodes()), ss.getLiveNodesWithPort());
                List<String> naturalEndpointsAddedPort = ss.getNaturalEndpoints(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                                                                SystemDistributedKeyspace.VIEW_BUILD_STATUS, "dummy").stream()
                                                           .map(e -> addStoragePortToIP(e.getHostAddress())).collect(Collectors.toList());
                Assert.assertEquals(naturalEndpointsAddedPort,
                                    ss.getNaturalEndpointsWithPort(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                                                   SystemDistributedKeyspace.VIEW_BUILD_STATUS, "dummy"));
                naturalEndpointsAddedPort = ss.getNaturalEndpoints(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, ByteBufferUtil.EMPTY_BYTE_BUFFER).stream()
                                              .map(e -> addStoragePortToIP(e.getHostAddress())).collect(Collectors.toList());
                Assert.assertEquals(naturalEndpointsAddedPort,
                                    ss.getNaturalEndpointsWithPort(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, ByteBufferUtil.EMPTY_BYTE_BUFFER));


                // Difference in key type... convert to String and add the port to the older format
                Map<String, Float> getOwnershipKeyAddedPort = ss.getOwnership().entrySet().stream()
                                                                .collect(Collectors.<Map.Entry<InetAddress, Float>, String, Float>toMap(
                                                                e -> addStoragePortToIP(e.getKey().toString()),
                                                                Map.Entry::getValue));
                Assert.assertEquals(getOwnershipKeyAddedPort, ss.getOwnershipWithPort());

                MessagingService ms = MessagingService.instance();
                Assert.assertEquals(addPortToKeys(ms.getTimeoutsPerHost()), ms.getTimeoutsPerHostWithPort());
                Assert.assertEquals(addPortToKeys(ms.getLargeMessagePendingTasks()), ms.getLargeMessagePendingTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getLargeMessageCompletedTasks()), ms.getLargeMessageCompletedTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getLargeMessageDroppedTasks()), ms.getLargeMessageDroppedTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getSmallMessagePendingTasks()), ms.getSmallMessagePendingTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getSmallMessageCompletedTasks()), ms.getSmallMessageCompletedTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getSmallMessageDroppedTasks()), ms.getSmallMessageDroppedTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getGossipMessagePendingTasks()), ms.getGossipMessagePendingTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getGossipMessageCompletedTasks()), ms.getGossipMessageCompletedTasksWithPort());
                Assert.assertEquals(addPortToKeys(ms.getGossipMessageDroppedTasks()), ms.getGossipMessageDroppedTasksWithPort());
            });
        }
    }


    final static Pattern IP4_ADDRESS = Pattern.compile("(127\\.0\\.\\d{1,3}\\.\\d{1,3})");

    static String addStoragePortToIP(String s)
    {
        return IP4_ADDRESS.matcher(s).replaceAll("$1:" + DatabaseDescriptor.getStoragePort());
    }

    static String addStoragePortToInstanceName(String s)
    {
        return Arrays.stream(s.split("\n")).map(line -> {
            if (line.startsWith(" "))
            {
                return line;
            }
            else // Host header line
            {
                return addStoragePortToIP(line);
            }
        }).collect(Collectors.joining("\n", "", "\n")); // to match final blank line
    }

    static <V> Map<String, V> addPortToKeys(Map<String, V> source)
    {
        return source.entrySet().stream().collect(Collectors.toMap(entry -> addStoragePortToIP(entry.getKey()),
                                                                   Map.Entry::getValue));
    }

    static <K> Map<K, String> addPortToValues(Map<K, String> source)
    {
        return source.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                                                   entry -> addStoragePortToIP(entry.getValue())));
    }

    static List<String> addPortToList(List<String> list)
    {
        return list.stream().map(GossipSettlesTest::addStoragePortToIP).collect(Collectors.toList());
    }

    static Map<String, List<String>> addPortToSchemaVersions(Map<String, List<String>> source)
    {
        return source.entrySet().stream()
                     .collect(Collectors.toMap(Map.Entry::getKey,
                                               hostAndPortEntry -> addPortToList(hostAndPortEntry.getValue())));
    }
}
