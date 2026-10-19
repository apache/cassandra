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

import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Verifies how {@code nodetool placement peers} maps its flags onto the MBean call. An omitted list flag is passed
 * as an empty set, which the MBean treats the same as null: no restriction.
 */
public class PlacementPeersMockTest extends AbstractNodetoolMock
{
    private static final String PAYLOAD = "version: 0\nformat_version: 1\nepoch: 7\n";

    @Test
    public void testPeersWithoutAnyOptions()
    {
        Invocation invocation = invoke("placement", "peers");
        assertThat(invocation.node).as("no node means every node").isNull();
        assertThat(invocation.datacenters).as("no datacenter restriction").isEmpty();
        assertThat(invocation.keyspaces).as("no keyspace selection").isEmpty();
        assertThat(invocation.excludedKeyspaces).as("no keyspace left out").isEmpty();
        assertThat(invocation.catchUpWithCMS).as("no catch-up unless asked").isFalse();
        assertThat(invocation.format).isEqualTo("yaml");
        assertThat(invocation.output).contains(PAYLOAD);
    }

    @Test
    public void testPeersWithNodeOption()
    {
        assertThat(invoke("placement", "peers", "-n", "3").node).isEqualTo("3");
        assertThat(invoke("placement", "peers", "--node", "127.0.0.3:7000").node).isEqualTo("127.0.0.3:7000");
    }

    @Test
    public void testPeersWithDatacenterOption()
    {
        assertThat(invoke("placement", "peers", "-dc", "dc1", "-dc", "dc2").datacenters)
            .containsExactlyInAnyOrder("dc1", "dc2");
        assertThat(invoke("placement", "peers", "--in-dc", "dc1").datacenters).containsExactly("dc1");
    }

    @Test
    public void testPeersWithMultipleKeyspaces()
    {
        Invocation invocation = invoke("placement", "peers", "-k", "ks1,ks2");
        assertThat(invocation.keyspaces).containsExactlyInAnyOrder("ks1", "ks2");
        assertThat(invocation.excludedKeyspaces).isEmpty();
    }

    @Test
    public void testPeersWithMultipleExcludeKeyspaces()
    {
        Invocation invocation = invoke("placement", "peers", "-x", "system_auth,system_traces");
        assertThat(invocation.excludedKeyspaces).containsExactlyInAnyOrder("system_auth", "system_traces");
        assertThat(invocation.keyspaces).isEmpty();
    }

    @Test
    public void testPeersWithCatchUpOption()
    {
        assertThat(invoke("placement", "peers", "--catch-up").catchUpWithCMS).isTrue();
    }

    @Test
    public void testPeersWithFormatOption()
    {
        Invocation invocation = invoke("placement", "peers", "-f", "json");
        assertThat(invocation.format).isEqualTo("json");
    }

    @Test
    public void testPeersWithKeyspaceCombinedWithExcludeKeyspaces()
    {
        assertFails("--keyspaces cannot be combined with --exclude-keyspaces",
                    "placement", "peers", "-k", "ks1", "-x", "system_auth");
    }

    @Test
    public void testPeersWithNodeCombinedWithDatacenter()
    {
        assertFails("--node cannot be combined with --in-dc", "placement", "peers", "-n", "3", "-dc", "dc1");
        assertFails("--node cannot be combined with --in-dc", "placement", "peers", "--node", "3", "--in-dc", "dc1");
    }

    private Invocation invoke(String... args)
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        // ArgumentMatchers is qualified because CQLTester inherits an unrelated any()
        when(mock.getReplicaPeers(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                                  ArgumentMatchers.any(), ArgumentMatchers.anyBoolean(),
                                  ArgumentMatchers.anyString()))
            .thenReturn(PAYLOAD);
        // the mock is shared across invocations, so forget earlier calls while keeping the stubbing above
        Mockito.clearInvocations(mock);

        ToolRunner.ToolResult result = invokeNodetool(args);
        result.assertOnCleanExit();

        ArgumentCaptor<String> node = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<String>> datacenters = captor();
        ArgumentCaptor<Set<String>> keyspaces = captor();
        ArgumentCaptor<Set<String>> excludedKeyspaces = captor();
        ArgumentCaptor<Boolean> catchUp = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<String> format = ArgumentCaptor.forClass(String.class);

        Mockito.verify(mock).getReplicaPeers(node.capture(), datacenters.capture(), keyspaces.capture(),
                                             excludedKeyspaces.capture(), catchUp.capture(),
                                             format.capture());

        return new Invocation(node.getValue(), datacenters.getValue(), keyspaces.getValue(),
                              excludedKeyspaces.getValue(), catchUp.getValue(),
                              format.getValue(), result.getStdout());
    }

    private void assertFails(String expectedMessage, String... args)
    {
        ToolRunner.ToolResult result = invokeNodetool(args);
        assertThat(result.getExitCode()).isEqualTo(1);
        assertThat(result.getStdout() + result.getStderr()).contains(expectedMessage);
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptor<Set<String>> captor()
    {
        return (ArgumentCaptor<Set<String>>) (ArgumentCaptor<?>) ArgumentCaptor.forClass(Set.class);
    }

    private static class Invocation
    {
        private final String node;
        private final Set<String> datacenters;
        private final Set<String> keyspaces;
        private final Set<String> excludedKeyspaces;
        private final boolean catchUpWithCMS;
        private final String format;
        private final String output;

        private Invocation(String node, Set<String> datacenters, Set<String> keyspaces, Set<String> excludedKeyspaces,
                           boolean catchUpWithCMS, String format, String output)
        {
            this.node = node;
            this.datacenters = datacenters;
            this.keyspaces = keyspaces;
            this.excludedKeyspaces = excludedKeyspaces;
            this.catchUpWithCMS = catchUpWithCMS;
            this.format = format;
            this.output = output;
        }
    }
}
