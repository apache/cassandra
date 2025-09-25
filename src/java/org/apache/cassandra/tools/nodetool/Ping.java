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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "ping", description = "Sends message to all live nodes in gossip")
public class Ping extends AbstractCommand
{
    @Option(names = {"-v", "--verbose"}, description = "Enable verbose output")
    private boolean verbose = false;

    @Option(names = {"-j", "--json"}, description = "Output results in JSON format")
    private boolean json = false;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            if (!json)
                probe.output().out.println("Sending ECHO_REQ to all live nodes...");

            // Use timing method for detailed results
            Map<String, Map<String, Object>> results = probe.getGossProxy().echoAllNodesWithTiming();

            if (json)
            {
                outputJson(probe, results);
            }
            else
            {
                outputTable(probe, results);
            }
        }
        catch (java.lang.Exception e)
        {
            if (json)
            {
                probe.output().out.println("{\"error\": \"" + e.getMessage() + "\"}");
            }
            else
            {
                probe.output().out.println("Failed to send echo requests: " + e.getMessage());
            }
        }
    }

    private void outputJson(NodeProbe probe, Map<String, Map<String, Object>> results)
    {
        probe.output().out.println("{");
        probe.output().out.println("  \"timestamp\": " + System.currentTimeMillis() + ",");
        probe.output().out.println("  \"nodes\": {");
        
        int nodeCount = 0;
        for (Entry<String, Map<String, Object>> entry : results.entrySet())
        {
            if (nodeCount > 0)
                probe.output().out.print(",");
            probe.output().out.println();
            
            String node = entry.getKey();
            Map<String, Object> nodeResult = entry.getValue();
            
            probe.output().out.printf("    \"%s\": {", node);
            probe.output().out.printf("\"status\": \"%s\"", nodeResult.get("status"));
            probe.output().out.printf(", \"responseTimeMs\": %d", ((Number) nodeResult.get("responseTimeMs")).longValue());
            probe.output().out.printf(", \"timestamp\": %d", nodeResult.get("timestamp"));
            
            if (nodeResult.containsKey("error"))
                probe.output().out.printf(", \"error\": \"%s\"", nodeResult.get("error"));
            
            probe.output().out.print("}");
            nodeCount++;
        }
        
        probe.output().out.println();
        probe.output().out.println("  },");
        
        long aliveCount = results.values().stream()
                                 .filter(nodeResult -> {
                                     String status = (String) nodeResult.get("status");
                                     return "ALIVE".equals(status) || "SELF".equals(status);
                                 })
                                 .count();
        
        probe.output().out.printf("  \"summary\": {\"alive\": %d, \"total\": %d}%n", aliveCount, results.size());
        probe.output().out.println("}");
    }

    private void outputTable(NodeProbe probe, Map<String, Map<String, Object>> results)
    {
        TableBuilder table = new TableBuilder();
        table.add("Node", "Status", "Response Time (ms)", "Details");
        
        for (Entry<String, Map<String, Object>> entry : results.entrySet())
        {
            String node = entry.getKey();
            Map<String, Object> nodeResult = entry.getValue();
            String status = (String) nodeResult.get("status");
            Long responseTimeMs = ((Number) nodeResult.get("responseTimeMs")).longValue();
            
            String details = "";
            if (nodeResult.containsKey("error"))
            {
                details = (String) nodeResult.get("error");
            }
            else if (verbose)
            {
                details = "ts: " + nodeResult.get("timestamp");
            }

            table.add(node, status, responseTimeMs.toString(), details);
        }
        
        table.printTo(probe.output().out);

        long aliveCount = results.values().stream()
                                 .filter(nodeResult -> {
                                     String status = (String) nodeResult.get("status");
                                     return "ALIVE".equals(status) || "SELF".equals(status);
                                 })
                                 .count();
        
        probe.output().out.printf("%nSummary: %d/%d nodes responded successfully%n", aliveCount, results.size());
    }
}
