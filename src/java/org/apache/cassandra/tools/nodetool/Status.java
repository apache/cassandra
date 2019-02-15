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

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.ToIntFunction;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import com.google.common.collect.ArrayListMultimap;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
@Command(name = "status", description = "Print cluster information (state, load, IDs, ...)")
public class Status extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace>]", description = "The keyspace name")
    private String keyspace = null;

    @Option(title = "resolve_ip", name = {"-r", "--resolve-ip"}, description = "Show node domain names instead of IPs")
    private boolean resolveIp = false;

    private boolean isTokenPerNode = true;
    private String format = null;
    private Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;
    private Map<String, String> loadMap, hostIDMap;
    private EndpointSnitchInfoMBean epSnitchInfo;

    @Override
    public void execute(NodeProbe probe)
    {
        joiningNodes = probe.getJoiningNodes(printPort);
        leavingNodes = probe.getLeavingNodes(printPort);
        movingNodes = probe.getMovingNodes(printPort);
        loadMap = probe.getLoadMap(printPort);
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap(printPort);
        liveNodes = probe.getLiveNodes(printPort);
        unreachableNodes = probe.getUnreachableNodes(printPort);
        hostIDMap = probe.getHostIdMap(printPort);
        epSnitchInfo = probe.getEndpointSnitchInfoProxy();

        StringBuilder errors = new StringBuilder();

        if (printPort)
        {
            Map<String, Float> ownerships = null;
            boolean hasEffectiveOwns = false;
            try
            {
                ownerships = probe.effectiveOwnershipWithPort(keyspace);
                hasEffectiveOwns = true;
            }
            catch (IllegalStateException e)
            {
                ownerships = probe.getOwnershipWithPort();
                errors.append("Note: ").append(e.getMessage()).append("%n");
            }
            catch (IllegalArgumentException ex)
            {
                System.out.printf("%nError: %s%n", ex.getMessage());
                System.exit(1);
            }

            SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

            // More tokens than nodes (aka vnodes)?
            if (dcs.size() < tokensToEndpoints.size())
                isTokenPerNode = false;

            int maxAddressLength = findMaxAddressLength(dcs, s -> s.ipOrDns().length());

            // Datacenters
            for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
            {
                String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
                System.out.print(dcHeader);
                for (int i = 0; i < (dcHeader.length() - 1); i++) System.out.print('=');
                System.out.println();

                // Legend
                System.out.println("Status=Up/Down");
                System.out.println("|/ State=Normal/Leaving/Joining/Moving");

                printNodesHeader(hasEffectiveOwns, isTokenPerNode, maxAddressLength);

                ArrayListMultimap<InetAddressAndPort, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
                for (HostStatWithPort stat : dc.getValue())
                    hostToTokens.put(stat.endpoint, stat);

                for (InetAddressAndPort endpoint : hostToTokens.keySet())
                {
                    Float owns = ownerships.get(endpoint.toString());
                    List<HostStatWithPort> tokens = hostToTokens.get(endpoint);
                    printNodeWithPort(endpoint.toString(), owns, tokens, hasEffectiveOwns, isTokenPerNode, maxAddressLength);
                }
            }

            System.out.printf("%n" + errors);
        }
        else
        {
            Map<InetAddress, Float> ownerships = null;
            boolean hasEffectiveOwns = false;
            try
            {
                ownerships = probe.effectiveOwnership(keyspace);
                hasEffectiveOwns = true;
            }
            catch (IllegalStateException e)
            {
                ownerships = probe.getOwnership();
                errors.append("Note: ").append(e.getMessage()).append("%n");
            }
            catch (IllegalArgumentException ex)
            {
                System.out.printf("%nError: %s%n", ex.getMessage());
                System.exit(1);
            }

            SortedMap<String, SetHostStat> dcs = NodeTool.getOwnershipByDc(probe, resolveIp, tokensToEndpoints, ownerships);

            // More tokens than nodes (aka vnodes)?
            if (dcs.values().size() < tokensToEndpoints.keySet().size())
                isTokenPerNode = false;

            int maxAddressLength = findMaxAddressLength(dcs, s -> s.ipOrDns().length());

            // Datacenters
            for (Map.Entry<String, SetHostStat> dc : dcs.entrySet())
            {
                String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
                System.out.print(dcHeader);
                for (int i = 0; i < (dcHeader.length() - 1); i++) System.out.print('=');
                System.out.println();

                // Legend
                System.out.println("Status=Up/Down");
                System.out.println("|/ State=Normal/Leaving/Joining/Moving");

                printNodesHeader(hasEffectiveOwns, isTokenPerNode, maxAddressLength);

                ArrayListMultimap<InetAddress, HostStat> hostToTokens = ArrayListMultimap.create();
                for (HostStat stat : dc.getValue())
                    hostToTokens.put(stat.endpoint, stat);

                for (InetAddress endpoint : hostToTokens.keySet())
                {
                    Float owns = ownerships.get(endpoint);
                    List<HostStat> tokens = hostToTokens.get(endpoint);
                    printNode(endpoint.getHostAddress(), owns, tokens, hasEffectiveOwns, isTokenPerNode, maxAddressLength);
                }
            }

            System.out.printf("%n" + errors);
        }
    }

    private <T extends Iterable<U>, U> int findMaxAddressLength(Map<String, T> dcs, ToIntFunction<U> computeLength)
    {
        int maxAddressLength = 0;

        Set<U> seenHosts = new HashSet<>();
        for (T stats : dcs.values())
            for (U stat : stats)
                if (seenHosts.add(stat))
                    maxAddressLength = Math.max(maxAddressLength, computeLength.applyAsInt(stat));

        return maxAddressLength;
    }

    private void printNodesHeader(boolean hasEffectiveOwns, boolean isTokenPerNode, int maxAddressLength)
    {
        String fmt = getFormat(hasEffectiveOwns, isTokenPerNode, maxAddressLength);
        String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

        if (isTokenPerNode)
            System.out.printf(fmt, "-", "-", "Address", "Load", owns, "Host ID", "Token", "Rack");
        else
            System.out.printf(fmt, "-", "-", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
    }

    private void printNode(String endpoint, Float owns, String epDns, String token, int size, boolean hasEffectiveOwns,
                           boolean isTokenPerNode, int maxAddressLength)
    {
        String status, state, load, strOwns, hostID, rack, fmt;
        fmt = getFormat(hasEffectiveOwns, isTokenPerNode, maxAddressLength);
        if (liveNodes.contains(endpoint)) status = "U";
        else if (unreachableNodes.contains(endpoint)) status = "D";
        else status = "?";
        if (joiningNodes.contains(endpoint)) state = "J";
        else if (leavingNodes.contains(endpoint)) state = "L";
        else if (movingNodes.contains(endpoint)) state = "M";
        else state = "N";

        load = loadMap.getOrDefault(endpoint, "?");
        strOwns = owns != null && hasEffectiveOwns ? new DecimalFormat("##0.0%").format(owns) : "?";
        hostID = hostIDMap.get(endpoint);

        try
        {
            rack = epSnitchInfo.getRack(endpoint);
        } catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        if (isTokenPerNode)
            System.out.printf(fmt, status, state, epDns, load, strOwns, hostID, token, rack);
        else
            System.out.printf(fmt, status, state, epDns, load, size, strOwns, hostID, rack);
    }

    private void printNode(String endpoint, Float owns, List<HostStat> tokens, boolean hasEffectiveOwns,
                           boolean isTokenPerNode, int maxAddressLength)
    {
        printNode(endpoint, owns, tokens.get(0).ipOrDns(), tokens.get(0).token, tokens.size(), hasEffectiveOwns,
                  isTokenPerNode, maxAddressLength);
    }

    private void printNodeWithPort(String endpoint, Float owns, List<HostStatWithPort> tokens, boolean hasEffectiveOwns,
                                   boolean isTokenPerNode, int maxAddressLength)
    {
        printNode(endpoint, owns, tokens.get(0).ipOrDns(), tokens.get(0).token, tokens.size(), hasEffectiveOwns,
                  isTokenPerNode, maxAddressLength);
    }

    private String getFormat(boolean hasEffectiveOwns, boolean isTokenPerNode, int maxAddressLength)
    {
        if (format == null)
        {
            StringBuilder buf = new StringBuilder();
            String addressPlaceholder = String.format("%%-%ds  ", maxAddressLength);
            buf.append("%s%s  ");                         // status
            buf.append(addressPlaceholder);               // address
            buf.append("%-9s  ");                         // load
            if (!isTokenPerNode)
                buf.append("%-11s  ");                     // "Tokens"
            if (hasEffectiveOwns)
                buf.append("%-16s  ");                    // "Owns (effective)"
            else
                buf.append("%-6s  ");                     // "Owns
            buf.append("%-36s  ");                        // Host ID
            if (isTokenPerNode)
                buf.append("%-39s  ");                    // token
            buf.append("%s%n");                           // "Rack"

            format = buf.toString();
        }

        return format;
    }
}
