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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

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
        TableBuilder tableBuilder = new TableBuilder("  ");

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

                addNodesHeader(hasEffectiveOwns, tableBuilder);

                ArrayListMultimap<InetAddressAndPort, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
                for (HostStatWithPort stat : dc.getValue())
                    hostToTokens.put(stat.endpoint, stat);

                for (InetAddressAndPort endpoint : hostToTokens.keySet())
                {
                    Float owns = ownerships.get(endpoint.toString());
                    List<HostStatWithPort> tokens = hostToTokens.get(endpoint);
                    addNodeWithPort(endpoint.toString(), owns, tokens, hasEffectiveOwns, tableBuilder);
                }
            }

            tableBuilder.printTo(System.out);
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

                addNodesHeader(hasEffectiveOwns, tableBuilder);

                ArrayListMultimap<InetAddress, HostStat> hostToTokens = ArrayListMultimap.create();
                for (HostStat stat : dc.getValue())
                    hostToTokens.put(stat.endpoint, stat);

                for (InetAddress endpoint : hostToTokens.keySet())
                {
                    Float owns = ownerships.get(endpoint);
                    List<HostStat> tokens = hostToTokens.get(endpoint);
                    addNode(endpoint.getHostAddress(), owns, tokens, hasEffectiveOwns, tableBuilder);
                }
            }

            tableBuilder.printTo(System.out);
            System.out.printf("%n" + errors);
        }
    }

    private void addNodesHeader(boolean hasEffectiveOwns, TableBuilder tableBuilder)
    {
        String owns = hasEffectiveOwns ? "Owns (effective)" : "Owns";

        if (isTokenPerNode)
            tableBuilder.add("--", "Address", "Load", owns, "Host ID", "Token", "Rack");
        else
            tableBuilder.add("--", "Address", "Load", "Tokens", owns, "Host ID", "Rack");
    }

    private void addNode(String endpoint, Float owns, String epDns, String token, int size, boolean hasEffectiveOwns,
                           TableBuilder tableBuilder)
    {
        String status, state, load, strOwns, hostID, rack;
        if (liveNodes.contains(endpoint)) status = "U";
        else if (unreachableNodes.contains(endpoint)) status = "D";
        else status = "?";
        if (joiningNodes.contains(endpoint)) state = "J";
        else if (leavingNodes.contains(endpoint)) state = "L";
        else if (movingNodes.contains(endpoint)) state = "M";
        else state = "N";

        String statusAndState = status.concat(state);
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
        {
            tableBuilder.add(statusAndState, epDns, load, strOwns, hostID, token, rack);
        }
        else
        {
            tableBuilder.add(statusAndState, epDns, load, String.valueOf(size), strOwns, hostID, rack);
        }
    }

    private void addNode(String endpoint, Float owns, List<HostStat> tokens, boolean hasEffectiveOwns,
                         TableBuilder tableBuilder)
    {
        addNode(endpoint, owns, tokens.get(0).ipOrDns(), tokens.get(0).token, tokens.size(), hasEffectiveOwns, tableBuilder);
    }

    private void addNodeWithPort(String endpoint, Float owns, List<HostStatWithPort> tokens, boolean hasEffectiveOwns,
                                 TableBuilder tableBuilder)
    {
        addNode(endpoint, owns, tokens.get(0).ipOrDns(), tokens.get(0).token, tokens.size(), hasEffectiveOwns, tableBuilder);
    }
}
