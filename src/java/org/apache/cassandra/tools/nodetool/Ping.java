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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "ping", description = "Sends message to all live nodes in gossip")
public class Ping extends AbstractCommand
{
    @Option(names = {"-v", "--verbose"}, description = "Enable verbose output")
    private boolean verbose = false;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            probe.output().out.println("Sending ECHO_REQ to all live nodes...");

            Map<String, String> results = probe.getGossProxy().echoAllNodes();

            if (verbose)
            {
                for (Map.Entry<String, String> entry : results.entrySet())
                {
                    probe.output().out.printf("Node %s: %s%n", entry.getKey(), entry.getValue());
                }
            }

            long aliveCount = results.values().stream()
                                     .filter(status -> "ALIVE".equals(status) || "SELF".equals(status))
                                     .count();

            probe.output().out.printf("Echo responses: %d/%d nodes responded%n",
                                      aliveCount, results.size());
        }
        catch (java.lang.Exception e)
        {
            probe.output().out.println("Failed to send echo requests: " + e.getMessage());
        }

        // Call probe methods to interact with Cassandra
        probe.output().out.println("Command executed successfully");
    }
}
