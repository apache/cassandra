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

import java.io.PrintStream;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.Iterator;
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
        PrintStream out = probe.output().out;
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
        TableBuilder.SharedTable sharedTable = new TableBuilder.SharedTable("  ");

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
            out.printf("%nError: %s%n", ex.getMessage());
            System.exit(1);
        }

        SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

        // More tokens than nodes (aka vnodes)?
        if (dcs.size() < tokensToEndpoints.size())
            isTokenPerNode = false;

        // Datacenters
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            TableBuilder tableBuilder = sharedTable.next();
            addNodesHeader(hasEffectiveOwns, tableBuilder);

            ArrayListMultimap<String, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
            for (HostStatWithPort stat : dc.getValue())
                hostToTokens.put(stat.ipOrDns(printPort), stat);

            for (String endpoint : hostToTokens.keySet())
            {
                Float owns = ownerships.get(endpoint);
                List<HostStatWithPort> tokens = hostToTokens.get(endpoint);
                addNode(endpoint, owns, tokens.get(0).ipOrDns(printPort), tokens.get(0).token, tokens.size(),
                        hasEffectiveOwns, tableBuilder);
            }
        }

        Iterator<TableBuilder> results = sharedTable.complete().iterator();
        boolean first = true;
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            if (!first) {
                out.println();
            }
            first = false;
            String dcHeader = String.format("Datacenter: %s%n", dc.getKey());
            out.print(dcHeader);
            for (int i = 0; i < (dcHeader.length() - 1); i++) out.print('=');
            out.println();

            // Legend
            out.println("Status=Up/Down");
            out.println("|/ State=Normal/Leaving/Joining/Moving");
            TableBuilder dcTable = results.next();
            dcTable.printTo(out);
        }

        out.printf("%n" + errors);
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
        String status, state, load, strOwns, hostID, rack, endpointIPAddr;

        try
        {
            if (resolveIp) endpointIPAddr = InetAddressAndPort.getByName(endpoint).getHostAddress(false);
            else endpointIPAddr = endpoint;

            rack = epSnitchInfo.getRack(endpointIPAddr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        if (liveNodes.contains(endpointIPAddr)) status = "U";
        else if (unreachableNodes.contains(endpointIPAddr)) status = "D";
        else status = "?";
        if (joiningNodes.contains(endpointIPAddr)) state = "J";
        else if (leavingNodes.contains(endpointIPAddr)) state = "L";
        else if (movingNodes.contains(endpointIPAddr)) state = "M";
        else state = "N";

        String statusAndState = status.concat(state);
        load = loadMap.getOrDefault(endpointIPAddr, "?");
        strOwns = owns != null && hasEffectiveOwns ? new DecimalFormat("##0.0%").format(owns) : "?";
        hostID = hostIDMap.get(endpointIPAddr);

        if (isTokenPerNode)
        {
            tableBuilder.add(statusAndState, epDns, load, strOwns, hostID, token, rack);
        }
        else
        {
            tableBuilder.add(statusAndState, epDns, load, String.valueOf(size), strOwns, hostID, rack);
        }
    }

}
