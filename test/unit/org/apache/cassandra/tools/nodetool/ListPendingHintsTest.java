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

package org.apache.cassandra.tools.nodetool;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.hints.PendingHintsInfo;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListPendingHintsTest
{
    private static final String HOST_ID = "11111111-1111-1111-1111-111111111111";

    private static Map<String, String> hint(String hostId)
    {
        Map<String, String> hintInfo = new HashMap<>();
        hintInfo.put(PendingHintsInfo.HOST_ID, hostId);
        hintInfo.put(PendingHintsInfo.TOTAL_FILES, "3");
        hintInfo.put(PendingHintsInfo.OLDEST_TIMESTAMP, "1000");
        hintInfo.put(PendingHintsInfo.NEWEST_TIMESTAMP, "2000");
        return hintInfo;
    }

    private static String runListPendingHints(List<Map<String, String>> pendingHints,
                                              Map<String, String> hostIdToEndpoint,
                                              Map<String, String> simpleStates,
                                              EndpointSnitchInfoMBean snitchInfo) throws UnsupportedEncodingException
    {
        NodeProbe probe = mock(NodeProbe.class);
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        Output output = new Output(new PrintStream(outBytes, true, StandardCharsets.UTF_8.name()),
                                   new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8.name()));

        when(probe.output()).thenReturn(output);
        when(probe.listPendingHints()).thenReturn(pendingHints);
        when(probe.getHostIdToEndpointWithPort()).thenReturn(hostIdToEndpoint);
        when(probe.getSimpleStatesWithPort()).thenReturn(simpleStates);
        when(probe.getEndpointSnitchInfoProxy()).thenReturn(snitchInfo);

        new ListPendingHints().execute(probe);

        return new String(outBytes.toByteArray(), StandardCharsets.UTF_8);
    }

    @Test
    public void testNoPendingHints() throws Exception
    {
        String out = runListPendingHints(Collections.emptyList(),
                                         Collections.emptyMap(),
                                         Collections.emptyMap(),
                                         mock(EndpointSnitchInfoMBean.class));

        assertTrue("Expected the no-pending-hints message but got: " + out,
                   out.contains("This node does not have any pending hints"));
    }

    @Test
    public void testResolvableHostId() throws Exception
    {
        Map<String, String> hostIdToEndpoint = new HashMap<>();
        hostIdToEndpoint.put(HOST_ID, "127.0.0.1:7000");

        Map<String, String> simpleStates = new HashMap<>();
        simpleStates.put("127.0.0.1:7000", "UP");

        EndpointSnitchInfoMBean snitchInfo = mock(EndpointSnitchInfoMBean.class);
        when(snitchInfo.getRack("127.0.0.1:7000")).thenReturn("rack1");
        when(snitchInfo.getDatacenter("127.0.0.1:7000")).thenReturn("dc1");

        String out = runListPendingHints(Collections.singletonList(hint(HOST_ID)),
                                         hostIdToEndpoint,
                                         simpleStates,
                                         snitchInfo);

        assertTrue("Expected host id in output but got: " + out, out.contains(HOST_ID));
        assertTrue("Expected address in output but got: " + out, out.contains("127.0.0.1:7000"));
        assertTrue("Expected rack in output but got: " + out, out.contains("rack1"));
        assertTrue("Expected dc in output but got: " + out, out.contains("dc1"));
    }

    /**
     * Covers the {@code catch (UnknownHostException)} branch: the address is present and resolves,
     * but a snitch lookup fails. Any location field already resolved before the failure (here the
     * rack) must be retained, while the remaining fields fall back to "Unknown".
     */
    @Test
    public void testUnknownHostExceptionRetainsResolvedFields() throws Exception
    {
        Map<String, String> hostIdToEndpoint = new HashMap<>();
        hostIdToEndpoint.put(HOST_ID, "127.0.0.1:7000");

        EndpointSnitchInfoMBean snitchInfo = mock(EndpointSnitchInfoMBean.class);
        when(snitchInfo.getRack("127.0.0.1:7000")).thenReturn("rack1");
        when(snitchInfo.getDatacenter("127.0.0.1:7000")).thenThrow(new UnknownHostException("boom"));

        String out = runListPendingHints(Collections.singletonList(hint(HOST_ID)),
                                         hostIdToEndpoint,
                                         Collections.emptyMap(),
                                         snitchInfo);

        assertTrue("Expected host id in output but got: " + out, out.contains(HOST_ID));
        assertTrue("Expected the already-resolved rack to be retained but got: " + out,
                   out.contains("rack1"));
        assertTrue("Expected dc/status to fall back to Unknown but got: " + out,
                   out.contains("Unknown"));
    }

    /**
     * Regression test: a hint may still be pending for a host id that is no longer part of the ring
     * (removed / decommissioned / replaced node). In that case the host-id-to-endpoint map has no
     * entry, the resolved address is null, and the command must not blow up with a
     * NullPointerException while trying to look up rack/dc/status for the null address.
     */
    @Test
    public void testUnknownHostIdDoesNotThrow() throws Exception
    {
        // Snitch would NPE on a null address; verify it is never invoked with null in the first place.
        EndpointSnitchInfoMBean snitchInfo = mock(EndpointSnitchInfoMBean.class);
        when(snitchInfo.getRack(anyString())).thenReturn("rack1");
        when(snitchInfo.getDatacenter(anyString())).thenReturn("dc1");

        String out = runListPendingHints(Collections.singletonList(hint(HOST_ID)),
                                         Collections.emptyMap(),
                                         Collections.emptyMap(),
                                         snitchInfo);

        assertTrue("Expected the pending hint host id to still be listed but got: " + out,
                   out.contains(HOST_ID));
        assertTrue("Expected unresolved location fields to be reported as Unknown but got: " + out,
                   out.contains("Unknown"));
    }

    @Test
    public void testNullHostIdDoesNotThrow() throws Exception
    {
        EndpointSnitchInfoMBean snitchInfo = mock(EndpointSnitchInfoMBean.class);

        // A null host id must not cause a NullPointerException either.
        String out = runListPendingHints(Collections.singletonList(hint(null)),
                                         Collections.emptyMap(),
                                         Collections.emptyMap(),
                                         snitchInfo);

        assertTrue("Expected unresolved fields to be reported as Unknown but got: " + out,
                   out.contains("Unknown"));
    }
}
