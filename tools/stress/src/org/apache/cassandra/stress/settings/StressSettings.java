package org.apache.cassandra.stress.settings;

import com.datastax.driver.core.Metadata;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.SimpleThriftClient;
import org.apache.cassandra.stress.util.SmartThriftClient;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StressSettings implements Serializable
{

    public final SettingsCommand command;
    public final SettingsRate rate;
    public final SettingsKey keys;
    public final SettingsColumn columns;
    public final SettingsLog log;
    public final SettingsMode mode;
    public final SettingsNode node;
    public final SettingsSchema schema;
    public final SettingsTransport transport;
    public final int thriftPort;
    public final int nativePort = 9042;
    public final String sendToDaemon;

    public StressSettings(SettingsCommand command, SettingsRate rate, SettingsKey keys, SettingsColumn columns, SettingsLog log, SettingsMode mode, SettingsNode node, SettingsSchema schema, SettingsTransport transport, int thriftPort, String sendToDaemon)
    {
        this.command = command;
        this.rate = rate;
        this.keys = keys;
        this.columns = columns;
        this.log = log;
        this.mode = mode;
        this.node = node;
        this.schema = schema;
        this.transport = transport;
        this.thriftPort = thriftPort;
        this.sendToDaemon = sendToDaemon;
    }

    public SmartThriftClient getSmartThriftClient()
    {
        Metadata metadata = getJavaDriverClient().getCluster().getMetadata();
        return new SmartThriftClient(this, schema.keyspace, metadata);
    }

    /**
     * Thrift client connection
     * @return cassandra client connection
     */
    public SimpleThriftClient getThriftClient()
    {
        return new SimpleThriftClient(getRawThriftClient(node.randomNode(), true));
    }

    public Cassandra.Client getRawThriftClient(boolean setKeyspace)
    {
        return getRawThriftClient(node.randomNode(), setKeyspace);
    }

    public Cassandra.Client getRawThriftClient(String host)
    {
        return getRawThriftClient(host, true);
    }

    public Cassandra.Client getRawThriftClient(String host, boolean setKeyspace)
    {

        TSocket socket = new TSocket(host, thriftPort);
        TTransport transport = this.transport.getFactory().getTransport(socket);
        Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(transport));

        try
        {
            if(!transport.isOpen())
                transport.open();

            if (mode.cqlVersion.isCql())
                client.set_cql_version(mode.cqlVersion.connectVersion);

            if (setKeyspace)
                client.set_keyspace("Keyspace1");
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }

        return client;
    }


    public SimpleClient getSimpleNativeClient()
    {
        try
        {
            String currentNode = node.randomNode();
            SimpleClient client = new SimpleClient(currentNode, nativePort);
            client.connect(false);
            client.execute("USE \"Keyspace1\";", org.apache.cassandra.db.ConsistencyLevel.ONE);
            return client;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static volatile JavaDriverClient client;

    public JavaDriverClient getJavaDriverClient()
    {
        if (client != null)
            return client;
        try
        {
            synchronized (this)
            {
                String currentNode = node.randomNode();
                if (client != null)
                    return client;
                JavaDriverClient c = new JavaDriverClient(currentNode, nativePort);
                c.connect(mode.compression);
                c.execute("USE \"Keyspace1\";", org.apache.cassandra.db.ConsistencyLevel.ONE);
                return client = c;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void maybeCreateKeyspaces()
    {
        if (command.type == Command.WRITE || command.type == Command.COUNTERWRITE)
            schema.createKeySpaces(this);

    }

    public static StressSettings parse(String[] args)
    {
        final Map<String, String[]> clArgs = parseMap(args);
        if (clArgs.containsKey("legacy"))
            return Legacy.build(Arrays.copyOfRange(args, 1, args.length));
        if (SettingsMisc.maybeDoSpecial(clArgs))
            System.exit(1);
        return get(clArgs);
    }

    public static StressSettings get(Map<String, String[]> clArgs)
    {
        SettingsCommand command = SettingsCommand.get(clArgs);
        if (command == null)
            throw new IllegalArgumentException("No command specified");
        int port = SettingsMisc.getPort(clArgs);
        String sendToDaemon = SettingsMisc.getSendToDaemon(clArgs);
        SettingsRate rate = SettingsRate.get(clArgs, command);
        SettingsKey keys = SettingsKey.get(clArgs, command);
        SettingsColumn columns = SettingsColumn.get(clArgs);
        SettingsLog log = SettingsLog.get(clArgs);
        SettingsMode mode = SettingsMode.get(clArgs);
        SettingsNode node = SettingsNode.get(clArgs);
        SettingsSchema schema = SettingsSchema.get(clArgs);
        SettingsTransport transport = SettingsTransport.get(clArgs);
        if (!clArgs.isEmpty())
        {
            printHelp();
            System.out.println("Error processing command line arguments. The following were ignored:");
            for (Map.Entry<String, String[]> e : clArgs.entrySet())
            {
                System.out.print(e.getKey());
                for (String v : e.getValue())
                {
                    System.out.print(" ");
                    System.out.print(v);
                }
                System.out.println();
            }
            System.exit(1);
        }
        return new StressSettings(command, rate, keys, columns, log, mode, node, schema, transport, port, sendToDaemon);
    }

    private static final Map<String, String[]> parseMap(String[] args)
    {
        // first is the main command/operation, so specified without a -
        if (args.length == 0)
        {
            System.out.println("No command provided");
            printHelp();
            System.exit(1);
        }
        final LinkedHashMap<String, String[]> r = new LinkedHashMap<>();
        String key = null;
        List<String> params = new ArrayList<>();
        for (int i = 0 ; i < args.length ; i++)
        {
            if (i == 0 || args[i].startsWith("-"))
            {
                if (i > 0)
                    r.put(key, params.toArray(new String[0]));
                key = args[i].toLowerCase();
                params.clear();
            }
            else
                params.add(args[i]);
        }
        r.put(key, params.toArray(new String[0]));
        return r;
    }

    public static void printHelp()
    {
        SettingsMisc.printHelp();
    }

    public void disconnect()
    {
        JavaDriverClient c;
        synchronized (this)
        {
            c = client;
            client = null;
        }
        c.disconnect();
    }
}
