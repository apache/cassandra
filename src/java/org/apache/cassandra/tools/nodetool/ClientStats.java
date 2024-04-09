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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
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

    @Option(title = "list_connections_with_client_options", name = "--client-options", description = "Lists all connections and the client options")
    private boolean clientOptions = false;

    @Option(title = "verbose", name = "--verbose", description = "Lists all connections with additional details (client options, authenticator-specific metadata and more)")
    private boolean verbose = false;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        if (clearConnectionHistory)
        {
            out.println("Clearing connection history");
            probe.clearConnectionHistory();
            return;
        }

        if (connectionsByProtocolVersion)
        {
            SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss");

            out.println("Clients by protocol version");
            out.println();

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

                table.printTo(out);
                out.println();
            }

            return;
        }

        // Note: for compatbility with existing implementation if someone passes --all (listConnections),
        // --client-options, and --metadata all three will be printed.
        List<Map<String, String>> clients = (List<Map<String, String>>) probe.getClientMetric("connections");
        if (!clients.isEmpty() && (listConnections || clientOptions || verbose))
        {
            ImmutableList.Builder<String> tableHeaderBuilder = ImmutableList.<String>builder()
                                                                            .add("Address", "SSL", "Cipher", "Protocol", "Version",
                                                                                 "User", "Keyspace", "Requests", "Driver-Name",
                                                                                 "Driver-Version");
            ImmutableList.Builder<String> tableFieldsBuilder = ImmutableList.<String>builder()
                                                                            .add(ConnectedClient.ADDRESS, ConnectedClient.SSL,
                                                                                 ConnectedClient.CIPHER, ConnectedClient.PROTOCOL,
                                                                                 ConnectedClient.VERSION, ConnectedClient.USER,
                                                                                 ConnectedClient.KEYSPACE, ConnectedClient.REQUESTS,
                                                                                 ConnectedClient.DRIVER_NAME, ConnectedClient.DRIVER_VERSION);
            if (clientOptions || verbose)
            {
                tableHeaderBuilder.add("Client-Options");
                tableFieldsBuilder.add(ConnectedClient.CLIENT_OPTIONS);
            }

            if (verbose)
            {
                tableHeaderBuilder.add("Auth-Mode", "Auth-Metadata");
                tableFieldsBuilder.add(ConnectedClient.AUTHENTICATION_MODE, ConnectedClient.AUTHENTICATION_METADATA);
            }

            List<String> tableHeader = tableHeaderBuilder.build();
            List<String> tableFields = tableFieldsBuilder.build();
            printTable(out, tableHeader, tableFields, clients);
        }

        Map<String, Integer> connectionsByUser = (Map<String, Integer>) probe.getClientMetric("connectedNativeClientsByUser");
        int total = connectionsByUser.values().stream().reduce(0, Integer::sum);
        out.println("Total connected clients: " + total);
        out.println();
        TableBuilder table = new TableBuilder();
        table.add("User", "Connections");
        for (Entry<String, Integer> entry : connectionsByUser.entrySet())
        {
            table.add(entry.getKey(), entry.getValue().toString());
        }
        table.printTo(out);
    }

    /**
     * Convenience function to print a table with the given header and the resolved fields for each connection.
     *
     * @param out         print stream to print to.
     * @param headers     headers for the table
     * @param tableFields the fields from {@link ConnectedClient} to retrieve from each client connection.
     * @param clients     the clients to print, each client being a row inthe table.
     */
    private void printTable(PrintStream out, List<String> headers, List<String> tableFields, List<Map<String, String>> clients)
    {
        TableBuilder table = new TableBuilder();
        table.add(headers);
        for (Map<String, String> conn : clients)
        {
            List<String> connectionFieldValues = tableFields.stream()
                    .map(conn::get)
                    .collect(Collectors.toList());
            table.add(connectionFieldValues);
        }
        table.printTo(out);
        out.println();
    }
}
