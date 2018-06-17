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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.transport.ClientStat;
import org.apache.cassandra.transport.ConnectedClient;

@Command(name = "clientstats", description = "Print information about connected clients")
public class ClientStats extends NodeToolCmd
{
    @Option(title = "list_connections", name = "--all", description = "Lists all connections")
    private boolean listConnections = false;

    @Option(title = "by_protocol", name = "--by-protocol", description = "Lists most recent client connections by protocol version")
    private boolean connectionsByProtocolVersion = false;

    @Option(title = "clear_history", name = "--clear-history", description = "Clear the history of connected clients")
    private boolean clearConnectionHistory = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (clearConnectionHistory)
        {
            System.out.println("Clearing connection history");
            probe.clearConnectionHistory();
            return;
        }

        if (connectionsByProtocolVersion)
        {
            SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss");

            System.out.println("Clients by protocol version");
            System.out.println();

            List<Map<String, String>> clients = (List<Map<String, String>>) probe.getClientMetric("clientsByProtocolVersion");

            if (!clients.isEmpty())
            {
                TableBuilder table = new TableBuilder();
                table.add("Protocol-Version", "IP-Address", "Last-Seen");

                for (Map<String, String> client : clients)
                {
                    table.add(client.get(ClientStat.PROTOCOL_VERSION),
                              client.get(ClientStat.INET_ADDRESS),
                              sdf.format(new Date(Long.valueOf(client.get(ClientStat.LAST_SEEN_TIME)))));
                }

                table.printTo(System.out);
                System.out.println();
            }

            return;
        }

        if (listConnections)
        {
            List<Map<String, String>> clients = (List<Map<String, String>>) probe.getClientMetric("connections");
            if (!clients.isEmpty())
            {
                TableBuilder table = new TableBuilder();
                table.add("Address", "SSL", "Cipher", "Protocol", "Version", "User", "Keyspace", "Requests", "Driver-Name", "Driver-Version");
                for (Map<String, String> conn : clients)
                {
                    table.add(conn.get(ConnectedClient.ADDRESS),
                              conn.get(ConnectedClient.SSL),
                              conn.get(ConnectedClient.CIPHER),
                              conn.get(ConnectedClient.PROTOCOL),
                              conn.get(ConnectedClient.VERSION),
                              conn.get(ConnectedClient.USER),
                              conn.get(ConnectedClient.KEYSPACE),
                              conn.get(ConnectedClient.REQUESTS),
                              conn.get(ConnectedClient.DRIVER_NAME),
                              conn.get(ConnectedClient.DRIVER_VERSION));
                }
                table.printTo(System.out);
                System.out.println();
            }
        }

        Map<String, Integer> connectionsByUser = (Map<String, Integer>) probe.getClientMetric("connectedNativeClientsByUser");
        int total = connectionsByUser.values().stream().reduce(0, Integer::sum);
        System.out.println("Total connected clients: " + total);
        System.out.println();
        TableBuilder table = new TableBuilder();
        table.add("User", "Connections");
        for (Entry<String, Integer> entry : connectionsByUser.entrySet())
        {
            table.add(entry.getKey(), entry.getValue().toString());
        }
        table.printTo(System.out);
    }
}