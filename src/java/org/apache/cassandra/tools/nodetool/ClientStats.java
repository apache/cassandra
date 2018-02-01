package org.apache.cassandra.tools.nodetool;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "clientstats", description = "Print information about connected clients")
public class ClientStats extends NodeToolCmd
{
    @Option(title = "list_connections", name = "--all", description = "Lists all connections")
    private boolean listConnections = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (listConnections)
        {
            List<Map<String, String>> clients = (List<Map<String, String>>) probe.getClientMetric("connections");
            if (!clients.isEmpty())
            {
                TableBuilder table = new TableBuilder();
                table.add("Address", "SSL", "Version", "User", "Keyspace", "Requests");
                for (Map<String, String> conn : clients)
                {
                    table.add(conn.get("address"), conn.get("ssl"), conn.get("version"), 
                              conn.get("user"), conn.get("keyspace"), conn.get("requests"));
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