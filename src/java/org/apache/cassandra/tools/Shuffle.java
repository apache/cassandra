package org.apache.cassandra.tools;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.cql.jdbc.JdbcDate;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class Shuffle extends AbstractJmxClient
{
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private static final String epSnitchObjName = "org.apache.cassandra.db:type=EndpointSnitchInfo";

    private StorageServiceMBean ssProxy = null;
    private Random rand = new Random(System.currentTimeMillis());
    private final String thriftHost;
    private final int thriftPort;
    private final boolean thriftFramed;

    static
    {
        addCmdOption("th",  "thrift-host",   true,  "Thrift hostname or IP address (Default: JMX host)");
        addCmdOption("tp",  "thrift-port",   true,  "Thrift port number (Default: 9160)");
        addCmdOption("tf",  "thrift-framed", false, "Enable framed transport for Thrift (Default: false)");
        addCmdOption("en",  "and-enable",    true,  "Immediately enable shuffling (create only)");
        addCmdOption("dc",  "only-dc",       true,  "Apply only to named DC (create only)");
    }

    public Shuffle(String host, int port) throws IOException
    {
        this(host, port, host, 9160, false, null, null);
    }

    public Shuffle(String host, int port, String thriftHost, int thriftPort, boolean thriftFramed, String username, String password)
    throws IOException
    {
        super(host, port, username, password);

        this.thriftHost = thriftHost;
        this.thriftPort = thriftPort;
        this.thriftFramed = thriftFramed;

        // Setup the StorageService proxy.
        ssProxy = getSSProxy(jmxConn.getMbeanServerConn());
    }

    public StorageServiceMBean getSSProxy(MBeanServerConnection mbeanConn)
    {
        StorageServiceMBean proxy = null;
        try
        {
            ObjectName name = new ObjectName(ssObjName);
            proxy = JMX.newMBeanProxy(mbeanConn, name, StorageServiceMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
        return proxy;
    }

    public EndpointSnitchInfoMBean getEpSnitchProxy(MBeanServerConnection mbeanConn)
    {
        EndpointSnitchInfoMBean proxy = null;
        try
        {
            ObjectName name = new ObjectName(epSnitchObjName);
            proxy = JMX.newMBeanProxy(mbeanConn, name, EndpointSnitchInfoMBean.class);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
        return proxy;
    }

    /**
     * Given a Multimap of endpoint to tokens, return a new randomized mapping.
     *
     * @param endpointMap current mapping of endpoint to tokens
     * @return a new mapping of endpoint to tokens
     */
    public Multimap<String, String> calculateRelocations(Multimap<String, String> endpointMap)
    {
        Multimap<String, String> relocations = HashMultimap.create();
        Set<String> endpoints = new HashSet<String>(endpointMap.keySet());
        Map<String, Integer> endpointToNumTokens = new HashMap<String, Integer>(endpoints.size());
        Map<String, Iterator<String>> iterMap = new HashMap<String, Iterator<String>>(endpoints.size());

        // Create maps of endpoint to token iterators, and endpoint to number of tokens.
        for (String endpoint : endpoints)
        {
            try
            {
                endpointToNumTokens.put(endpoint, ssProxy.getTokens(endpoint).size());
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException("What that...?", e);
            }

            iterMap.put(endpoint, endpointMap.get(endpoint).iterator());
        }

        int epsToComplete = endpoints.size();
        Set<String> endpointsCompleted = new HashSet<String>();

        outer:
        while (true)
        {
            endpoints.removeAll(endpointsCompleted);

            for (String endpoint : endpoints)
            {
                boolean choiceMade = false;

                if (!iterMap.get(endpoint).hasNext())
                {
                    endpointsCompleted.add(endpoint);
                    continue;
                }

                String token = iterMap.get(endpoint).next();

                List<String> subSet = new ArrayList<String>(endpoints);
                subSet.remove(endpoint);
                Collections.shuffle(subSet, rand);

                for (String choice : subSet)
                {
                    if (relocations.get(choice).size() < endpointToNumTokens.get(choice))
                    {
                        relocations.put(choice, token);
                        choiceMade = true;
                        break;
                    }
                }

                if (!choiceMade)
                    relocations.put(endpoint, token);
            }

            // We're done when we've exhausted all of the token iterators
            if (endpointsCompleted.size() == epsToComplete)
                break outer;
        }

        return relocations;
    }

    /**
     * Enable relocations.
     *
     * @param endpoints sequence of hostname or IP strings
     */
    public void enableRelocations(String...endpoints)
    {
        enableRelocations(Arrays.asList(endpoints));
    }

    /**
     * Enable relocations.
     *
     * @param endpoints Collection of hostname or IP strings
     */
    public void enableRelocations(Collection<String> endpoints)
    {
        for (String endpoint : endpoints)
        {
            try
            {
                JMXConnection conn = new JMXConnection(endpoint, port);
                getSSProxy(conn.getMbeanServerConn()).enableScheduledRangeXfers();
                conn.close();
            }
            catch (IOException e)
            {
                writeln("Failed to enable shuffling on %s!", endpoint);
            }
        }
    }

    /**
     * Disable relocations.
     *
     * @param endpoints sequence of hostname or IP strings
     */
    public void disableRelocations(String...endpoints)
    {
        disableRelocations(Arrays.asList(endpoints));
    }

    /**
     * Disable relocations.
     *
     * @param endpoints Collection of hostname or IP strings
     */
    public void disableRelocations(Collection<String> endpoints)
    {
        for (String endpoint : endpoints)
        {
            try
            {
                JMXConnection conn = new JMXConnection(endpoint, port);
                getSSProxy(conn.getMbeanServerConn()).disableScheduledRangeXfers();
                conn.close();
            }
            catch (IOException e)
            {
                writeln("Failed to enable shuffling on %s!", endpoint);
            }
        }
    }

    /**
     * Return a list of the live nodes (using JMX).
     *
     * @return String endpoint names
     * @throws ShuffleError
     */
    public Collection<String> getLiveNodes() throws ShuffleError
    {
        try
        {
            JMXConnection conn = new JMXConnection(host, port);
            return getSSProxy(conn.getMbeanServerConn()).getLiveNodes();
        }
        catch (IOException e)
        {
            throw new ShuffleError("Error retrieving list of nodes from JMX interface");
        }
    }

    /**
     * Create and distribute a new, randomized token to endpoint mapping.
     *
     * @throws ShuffleError on handled exceptions
     */
    public void shuffle(boolean enable, String onlyDc) throws ShuffleError
    {
        CassandraClient seedClient = null;
        Map<String, String> tokenMap = null;
        IPartitioner<?> partitioner = null;
        Multimap<String, String> endpointMap = HashMultimap.create();
        EndpointSnitchInfoMBean epSnitchProxy = getEpSnitchProxy(jmxConn.getMbeanServerConn());

        try
        {
            seedClient = getThriftClient(thriftHost, thriftPort, thriftFramed);
            tokenMap = seedClient.describe_token_map();

            for (Map.Entry<String, String> entry : tokenMap.entrySet())
            {
                String endpoint = entry.getValue(), token = entry.getKey();
                try
                {
                    if (onlyDc != null)
                    {
                        if (onlyDc.equals(epSnitchProxy.getDatacenter(endpoint)))
                            endpointMap.put(endpoint, token);
                    }
                    else
                        endpointMap.put(endpoint, token);
                }
                catch (UnknownHostException e)
                {
                    writeln("Warning: %s unknown to EndpointSnitch!", endpoint);
                }
            }
        }
        catch (InvalidRequestException ire)
        {
            throw new RuntimeException("What that...?", ire);
        }
        catch (TException e)
        {
            throw new ShuffleError(
                    String.format("Thrift request to %s:%d failed: %s", thriftHost, thriftPort, e.getMessage()));
        }

        partitioner = getPartitioner(thriftHost, thriftPort, thriftFramed);

        Multimap<String, String> relocations = calculateRelocations(endpointMap);

        writeln("%-42s %-15s %-15s", "Token", "From", "To");
        writeln("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+~~~~~~~~~~~~~~~+~~~~~~~~~~~~~~~");

        // Store relocations on remote nodes.
        for (String endpoint : relocations.keySet())
        {
            for (String tok : relocations.get(endpoint))
                writeln("%-42s %-15s %-15s", tok, tokenMap.get(tok), endpoint);

            String cqlQuery = createShuffleBatchInsert(relocations.get(endpoint), partitioner);
            executeCqlQuery(endpoint, thriftPort, thriftFramed, cqlQuery);
        }

        if (enable)
            enableRelocations(relocations.keySet());
    }

    /**
     * Print a list of pending token relocations for all nodes.
     *
     * @throws ShuffleError
     */
    public void ls() throws ShuffleError
    {
        Map<String, List<CqlRow>> queuedRelocations = listRelocations();
        IPartitioner<?> partitioner = getPartitioner(thriftHost, thriftPort, thriftFramed);
        boolean justOnce = false;

        for (String host : queuedRelocations.keySet())
        {
            for (CqlRow row : queuedRelocations.get(host))
            {
                assert row.getColumns().size() == 2;

                if (!justOnce)
                {
                    writeln("%-42s %-15s %s", "Token", "Endpoint", "Requested at");
                    writeln("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~+~~~~~~~~~~~~~~~+~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                    justOnce = true;
                }

                ByteBuffer tokenBytes = ByteBuffer.wrap(row.getColumns().get(0).getValue());
                ByteBuffer requestedAt = ByteBuffer.wrap(row.getColumns().get(1).getValue());
                Date time = JdbcDate.instance.compose(requestedAt);
                Token<?> token = partitioner.getTokenFactory().fromByteArray(tokenBytes);

                writeln("%-42s %-15s %s", token.toString(), host, time.toString());
            }
        }
    }

    /**
     * List pending token relocations for all nodes.
     *
     * @return
     * @throws ShuffleError
     */
    private Map<String, List<CqlRow>> listRelocations() throws ShuffleError
    {
        String cqlQuery = "SELECT token_bytes,requested_at FROM system.range_xfers";
        Map<String, List<CqlRow>> results = new HashMap<String, List<CqlRow>>();

        for (String host : getLiveNodes())
        {
            CqlResult result = executeCqlQuery(host, thriftPort, thriftFramed, cqlQuery);
            results.put(host, result.getRows());
        }

        return results;
    }

    /**
     * Clear pending token relocations on all nodes.
     *
     * @throws ShuffleError
     */
    public void clear() throws ShuffleError
    {
        Map<String, List<CqlRow>> queuedRelocations = listRelocations();

        for (String host : queuedRelocations.keySet())
        {

            for (CqlRow row : queuedRelocations.get(host))
            {
                assert row.getColumns().size() == 2;

                ByteBuffer tokenBytes = ByteBuffer.wrap(row.getColumns().get(0).getValue());
                String query = String.format("DELETE FROM system.range_xfers WHERE token_bytes = '%s'",
                        ByteBufferUtil.bytesToHex(tokenBytes));
                executeCqlQuery(host, thriftPort, thriftFramed, query);
            }
        }
    }

    /**
     * Enable shuffling on all nodes in the cluster.
     *
     * @throws ShuffleError
     */
    public void enable() throws ShuffleError
    {
        enableRelocations(getLiveNodes());
    }

    /**
     * Disable shuffling on all nodes in the cluster.
     *
     * @throws ShuffleError
     */
    public void disable() throws ShuffleError
    {
        disableRelocations(getLiveNodes());
    }

    /**
     * Setup and return a new Thrift RPC connection.
     *
     * @param hostName hostname or address to connect to
     * @param port port number to connect to
     * @param framed wrap with framed transport if true
     * @return a CassandraClient instance
     * @throws ShuffleError
     */
    public static CassandraClient getThriftClient(String hostName, int port, boolean framed) throws ShuffleError
    {
        try
        {
            return new CassandraClient(hostName, port, framed);
        }
        catch (TTransportException e)
        {
            throw new ShuffleError(String.format("Unable to connect to %s/%d: %s", hostName, port, e.getMessage()));
        }
    }

    /**
     * Execute a CQL v3 query.
     *
     * @param hostName hostname or address to connect to
     * @param port port number to connect to
     * @param isFramed wrap with framed transport if true
     * @param cqlQuery CQL query string
     * @return a Thrift CqlResult instance
     * @throws ShuffleError
     */
    public static CqlResult executeCqlQuery(String hostName, int port, boolean isFramed, String cqlQuery) throws ShuffleError
    {
        CassandraClient client = null;

        try
        {
            client = getThriftClient(hostName, port, isFramed);
            return client.execute_cql_query(ByteBuffer.wrap(cqlQuery.getBytes()), Compression.NONE);
        }
        catch (UnavailableException e)
        {
            throw new ShuffleError(
                    String.format("Unable to write shuffle entries to %s. Reason: UnavailableException", hostName));
        }
        catch (TimedOutException e)
        {
            throw new ShuffleError(
                    String.format("Unable to write shuffle entries to %s. Reason: TimedOutException", hostName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (client != null)
                client.close();
        }
    }

    /**
     * Return a partitioner instance for remote host.
     *
     * @param hostName hostname or address to connect to
     * @param port port number to connect to
     * @param framed wrap with framed transport if true
     * @return an IPartitioner instance
     * @throws ShuffleError
     */
    public static IPartitioner<?> getPartitioner(String hostName, int port, boolean framed) throws ShuffleError
    {
        String partitionerName = null;
        try
        {
            partitionerName = getThriftClient(hostName, port, framed).describe_partitioner();
        }
        catch (TException e)
        {
            throw new ShuffleError(
                    String.format("Thrift request to %s:%d failed: %s", hostName, port, e.getMessage()));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException("Error calling describe_partitioner() defies explanation", e);
        }

        try
        {
            Class<?> partitionerClass = Class.forName(partitionerName);
            return (IPartitioner<?>)partitionerClass.newInstance();
        }
        catch (ClassNotFoundException e)
        {
            throw new ShuffleError("Unable to locate class for partitioner: " + partitionerName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create and return a CQL batch insert statement for a set of token relocations.
     *
     * @param tokens tokens to be relocated
     * @param partitioner an instance of the IPartitioner in use
     * @return a query string
     */
    public static String createShuffleBatchInsert(Collection<String> tokens, IPartitioner<?> partitioner)
    {
        StringBuilder query = new StringBuilder();
        query.append("BEGIN BATCH").append("\n");

        for (String tokenStr : tokens)
        {
            Token<?> token = partitioner.getTokenFactory().fromString(tokenStr);
            String hexToken = ByteBufferUtil.bytesToHex(partitioner.getTokenFactory().toByteArray(token));
            query.append("INSERT INTO system.range_xfers (token_bytes, requested_at) ")
                 .append("VALUES ('").append(hexToken).append("', 'now');").append("\n");
        }

        query.append("APPLY BATCH").append("\n");
        return query.toString();
    }

    /** Print usage information. */
    private static void printShuffleHelp()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Sub-commands:").append(String.format("%n"));
        sb.append(" create           Initialize a new shuffle operation").append(String.format("%n"));
        sb.append(" ls               List pending relocations").append(String.format("%n"));
        sb.append(" clear            Clear pending relocations").append(String.format("%n"));
        sb.append(" en[able]         Enable shuffling").append(String.format("%n"));
        sb.append(" dis[able]        Disable shuffling").append(String.format("%n%n"));

        printHelp("shuffle [options] <sub-command>", sb.toString());
    }

    /**
     * Execute.
     *
     * @param args arguments passed on the command line
     * @throws Exception when face meets palm
     */
    public static void main(String[] args) throws Exception
    {
        CommandLine cmd = null;
        try
        {
            cmd = processArguments(args);
        }
        catch (MissingArgumentException e)
        {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        // Sub command argument.
        if (cmd.getArgList().size() < 1)
        {
            System.err.println("Missing sub-command argument.");
            printShuffleHelp();
            System.exit(1);
        }
        String subCommand = (String)(cmd.getArgList()).get(0);

        String hostName = (cmd.getOptionValue("host") != null) ? cmd.getOptionValue("host") : DEFAULT_HOST;
        String port = (cmd.getOptionValue("port") != null) ? cmd.getOptionValue("port") : Integer.toString(DEFAULT_JMX_PORT);
        String thriftHost = (cmd.getOptionValue("thrift-host") != null) ? cmd.getOptionValue("thrift-host") : hostName;
        String thriftPort = (cmd.getOptionValue("thrift-port") != null) ? cmd.getOptionValue("thrift-port") : "9160";
        String onlyDc = cmd.getOptionValue("only-dc");
        boolean thriftFramed = cmd.hasOption("thrift-framed") ? true : false;
        boolean andEnable = cmd.hasOption("and-enable") ? true : false;
        int portNum = -1, thriftPortNum = -1;

        // Parse JMX port number
        if (port != null)
        {
            try
            {
                portNum = Integer.parseInt(port);
            }
            catch (NumberFormatException ferr)
            {
                System.err.printf("%s is not a valid JMX port number.%n", port);
                System.exit(1);
            }
        }
        else
            portNum = DEFAULT_JMX_PORT;

        // Parse Thrift port number
        if (thriftPort != null)
        {
            try
            {
                thriftPortNum = Integer.parseInt(thriftPort);
            }
            catch (NumberFormatException ferr)
            {
                System.err.printf("%s is not a valid port number.%n", thriftPort);
                System.exit(1);
            }
        }
        else
            thriftPortNum = 9160;

        Shuffle shuffler = new Shuffle(hostName, portNum, thriftHost, thriftPortNum, thriftFramed, null, null);

        try
        {
            if (subCommand.equals("create"))
                shuffler.shuffle(andEnable, onlyDc);
            else if (subCommand.equals("ls"))
                shuffler.ls();
            else if (subCommand.startsWith("en"))
                shuffler.enable();
            else if (subCommand.startsWith("dis"))
                shuffler.disable();
            else if (subCommand.equals("clear"))
                shuffler.clear();
            else
            {
                System.err.println("Unknown subcommand: " + subCommand);
                printShuffleHelp();
                System.exit(1);
            }
        }
        catch (ShuffleError err)
        {
            shuffler.writeln(err);
            System.exit(1);
        }
        finally
        {
            shuffler.close();
        }

        System.exit(0);
    }
}

/** A self-contained Cassandra.Client; Closeable. */
class CassandraClient implements Closeable
{
    TTransport transport;
    Cassandra.Client client;

    CassandraClient(String hostName, int port, boolean framed) throws TTransportException
    {
        TSocket socket = new TSocket(hostName, port);
        transport = (framed) ? socket : new TFastFramedTransport(socket);
        transport.open();
        client = new Cassandra.Client(new TBinaryProtocol(transport));

        try
        {
            client.set_cql_version("3.0.0");
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    CqlResult execute_cql_query(ByteBuffer cqlQuery, Compression compression) throws Exception
    {
        return client.execute_cql3_query(cqlQuery, compression, ConsistencyLevel.ONE);
    }

    String describe_partitioner() throws TException, InvalidRequestException
    {
        return client.describe_partitioner();
    }

    List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException, TException
    {
        return client.describe_ring(keyspace);
    }

    Map<String, String> describe_token_map() throws InvalidRequestException, TException
    {
        return client.describe_token_map();
    }

    public void close()
    {
        transport.close();
    }
}

@SuppressWarnings("serial")
class ShuffleError extends Exception
{
    ShuffleError(String msg)
    {
        super(msg);
    }
}
