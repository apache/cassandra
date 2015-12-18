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

import static java.lang.String.format;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import com.google.common.collect.LinkedHashMultimap;

@Command(name = "ring", description = "Print information about the token ring")
public class Ring extends NodeToolCmd
{
    @Arguments(description = "Specify a keyspace for accurate ownership information (topology awareness)")
    private String keyspace = null;

    @Option(title = "resolve_ip", name = {"-r", "--resolve-ip"}, description = "Show node domain names instead of IPs")
    private boolean resolveIp = false;

    @Override
    public void execute(NodeProbe probe)
    {
        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
        LinkedHashMultimap<String, String> endpointsToTokens = LinkedHashMultimap.create();
        boolean haveVnodes = false;
        for (Map.Entry<String, String> entry : tokensToEndpoints.entrySet())
        {
            haveVnodes |= endpointsToTokens.containsKey(entry.getValue());
            endpointsToTokens.put(entry.getValue(), entry.getKey());
        }

        int maxAddressLength = Collections.max(endpointsToTokens.keys(), new Comparator<String>()
        {
            @Override
            public int compare(String first, String second)
            {
            	return Integer.compare(first.length(), second.length());
            }
        }).length();

        String formatPlaceholder = "%%-%ds  %%-12s%%-7s%%-8s%%-16s%%-20s%%-44s%%n";
        String format = format(formatPlaceholder, maxAddressLength);

        StringBuffer errors = new StringBuffer();
        boolean showEffectiveOwnership = true;
        // Calculate per-token ownership of the ring
        Map<InetAddress, Float> ownerships;
        try
        {
            ownerships = probe.effectiveOwnership(keyspace);
        }
        catch (IllegalStateException ex)
        {
            ownerships = probe.getOwnership();
            errors.append("Note: " + ex.getMessage() + "%n");
            showEffectiveOwnership = false;
        }
        catch (IllegalArgumentException ex)
        {
            System.out.printf("%nError: %s%n", ex.getMessage());
            return;
        }


        System.out.println();
        for (Entry<String, SetHostStat> entry : NodeTool.getOwnershipByDc(probe, resolveIp, tokensToEndpoints, ownerships).entrySet())
            printDc(probe, format, entry.getKey(), endpointsToTokens, entry.getValue(),showEffectiveOwnership);

        if (haveVnodes)
        {
            System.out.println("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
            System.out.println("  To view status related info of a node use \"nodetool status\" instead.\n");
        }

        System.out.printf("%n  " + errors.toString());
    }

    private void printDc(NodeProbe probe, String format,
                         String dc,
                         LinkedHashMultimap<String, String> endpointsToTokens,
                         SetHostStat hoststats,boolean showEffectiveOwnership)
    {
        Collection<String> liveNodes = probe.getLiveNodes();
        Collection<String> deadNodes = probe.getUnreachableNodes();
        Collection<String> joiningNodes = probe.getJoiningNodes();
        Collection<String> leavingNodes = probe.getLeavingNodes();
        Collection<String> movingNodes = probe.getMovingNodes();
        Map<String, String> loadMap = probe.getLoadMap();

        System.out.println("Datacenter: " + dc);
        System.out.println("==========");

        // get the total amount of replicas for this dc and the last token in this dc's ring
        List<String> tokens = new ArrayList<>();
        String lastToken = "";

        for (HostStat stat : hoststats)
        {
            tokens.addAll(endpointsToTokens.get(stat.endpoint.getHostAddress()));
            lastToken = tokens.get(tokens.size() - 1);
        }

        System.out.printf(format, "Address", "Rack", "Status", "State", "Load", "Owns", "Token");

        if (hoststats.size() > 1)
            System.out.printf(format, "", "", "", "", "", "", lastToken);
        else
            System.out.println();

        for (HostStat stat : hoststats)
        {
            String endpoint = stat.endpoint.getHostAddress();
            String rack;
            try
            {
                rack = probe.getEndpointSnitchInfoProxy().getRack(endpoint);
            }
            catch (UnknownHostException e)
            {
                rack = "Unknown";
            }

            String status = liveNodes.contains(endpoint)
                    ? "Up"
                    : deadNodes.contains(endpoint)
                            ? "Down"
                            : "?";

            String state = "Normal";

            if (joiningNodes.contains(endpoint))
                state = "Joining";
            else if (leavingNodes.contains(endpoint))
                state = "Leaving";
            else if (movingNodes.contains(endpoint))
                state = "Moving";

            String load = loadMap.containsKey(endpoint)
                    ? loadMap.get(endpoint)
                    : "?";
            String owns = stat.owns != null && showEffectiveOwnership? new DecimalFormat("##0.00%").format(stat.owns) : "?";
            System.out.printf(format, stat.ipOrDns(), rack, status, state, load, owns, stat.token);
        }
        System.out.println();
    }
}
