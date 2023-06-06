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

import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.ArrayListMultimap;

import io.airlift.airline.Command;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "describecluster", description = "Print the name, snitch, partitioner and schema version of a cluster")
public class DescribeCluster extends NodeToolCmd
{
    private boolean resolveIp = false;
    private String keyspace = null;
    private Collection<String> joiningNodes, leavingNodes, movingNodes, liveNodes, unreachableNodes;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        // display cluster name, snitch and partitioner
        out.println("Cluster Information:");
        out.println("\tName: " + probe.getClusterName());
        String snitch = probe.getEndpointSnitchInfoProxy().getSnitchName();
        boolean dynamicSnitchEnabled = false;
        if (snitch.equals(DynamicEndpointSnitch.class.getName()))
        {
            snitch = probe.getDynamicEndpointSnitchInfoProxy().getSubsnitchClassName();
            dynamicSnitchEnabled = true;
        }
        out.println("\tSnitch: " + snitch);
        out.println("\tDynamicEndPointSnitch: " + (dynamicSnitchEnabled ? "enabled" : "disabled"));
        out.println("\tPartitioner: " + probe.getPartitioner());

        // display schema version for each node
        out.println("\tSchema versions:");
        Map<String, List<String>> schemaVersions = printPort ? probe.getSpProxy().getSchemaVersionsWithPort() : probe.getSpProxy().getSchemaVersions();
        for (Map.Entry<String, List<String>> entry : schemaVersions.entrySet())
        {
            out.printf("\t\t%s: %s%n%n", entry.getKey(), entry.getValue());
        }

        // Collect status information of all nodes
        boolean withPort = true;
        joiningNodes = probe.getJoiningNodes(withPort);
        leavingNodes = probe.getLeavingNodes(withPort);
        movingNodes = probe.getMovingNodes(withPort);
        liveNodes = probe.getLiveNodes(withPort);
        unreachableNodes = probe.getUnreachableNodes(withPort);

        // Get the list of all keyspaces
        List<String> keyspaces = probe.getKeyspaces();

        out.println("Stats for all nodes:");
        out.println("\tLive: " + liveNodes.size());
        out.println("\tJoining: " + joiningNodes.size());
        out.println("\tMoving: " + movingNodes.size());
        out.println("\tLeaving: " + leavingNodes.size());
        out.println("\tUnreachable: " + unreachableNodes.size());

        Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap(withPort);
        StringBuilder errors = new StringBuilder();
        Map<String, Float> ownerships = null;
        try
        {
            ownerships = probe.effectiveOwnershipWithPort(keyspace);
        }
        catch (IllegalStateException ex)
        {
            try
            {
                ownerships = probe.getOwnershipWithPort();
                errors.append("Note: ").append(ex.getMessage()).append("%n");
            }
            catch (Exception e)
            {
                out.printf("%nError: %s%n", e.getMessage());
                System.exit(1);
            }
        }
        catch (IllegalArgumentException ex)
        {
            out.printf("%nError: %s%n", ex.getMessage());
            System.exit(1);
        }

        SortedMap<String, SetHostStatWithPort> dcs = NodeTool.getOwnershipByDcWithPort(probe, resolveIp, tokensToEndpoints, ownerships);

        out.println("\nData Centers: ");
        for (Map.Entry<String, SetHostStatWithPort> dc : dcs.entrySet())
        {
            out.print('\t' + dc.getKey());

            ArrayListMultimap<InetAddressAndPort, HostStatWithPort> hostToTokens = ArrayListMultimap.create();
            for (HostStatWithPort stat : dc.getValue())
                hostToTokens.put(stat.endpointWithPort, stat);

            int totalNodes = 0; // total number of nodes in a datacenter
            int downNodes = 0; // number of down nodes in a datacenter

            for (InetAddressAndPort endpoint : hostToTokens.keySet())
            {
                totalNodes++;
                if (unreachableNodes.contains(endpoint.getHostAddressAndPort()))
                    downNodes++;
            }
            out.print(" #Nodes: " + totalNodes);
            out.println(" #Down: " + downNodes);
        }

        // display database version for each node
        out.println("\nDatabase versions:");
        Map<String, List<String>> databaseVersions = probe.getGossProxy().getReleaseVersionsWithPort();
        for (Map.Entry<String, List<String>> entry : databaseVersions.entrySet())
        {
            out.printf("\t%s: %s%n%n", entry.getKey(), entry.getValue());
        }

        out.println("Keyspaces:");
        for (String keyspaceName : keyspaces)
        {
            String replicationInfo = probe.getKeyspaceReplicationInfo(keyspaceName);
            if (replicationInfo == null)
            {
                out.println("something went wrong for keyspace: " + keyspaceName);
            }
            out.printf("\t%s -> Replication class: %s%n", keyspaceName, replicationInfo);
        }

        if (errors.length() != 0)
            out.printf("%n" + errors);
    }
}
