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
package org.apache.cassandra.tools;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.commons.cli.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

public class BulkLoader
{
    private static final String TOOL_NAME = "sstableloader";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String NOPROGRESS_OPTION  = "no-progress";
    private static final String IGNORE_NODES_OPTION  = "ignore";
    private static final String INITIAL_HOST_ADDRESS_OPTION = "nodes";
    private static final String RPC_PORT_OPTION = "port";
    private static final String USER_OPTION = "username";
    private static final String PASSWD_OPTION = "password";
    private static final String THROTTLE_MBITS = "throttle";
    private static final String TRANSPORT_FACTORY = "transport-factory";
    private static final String SSL_TRUSTSTORE = "truststore";
    private static final String SSL_TRUSTSTORE_PW = "truststore-password";
    private static final String SSL_KEYSTORE = "keystore";
    private static final String SSL_KEYSTORE_PW = "keystore-password";
    private static final String SSL_PROTOCOL = "ssl-protocol";
    private static final String SSL_ALGORITHM = "ssl-alg";
    private static final String SSL_STORE_TYPE = "store-type";
    private static final String SSL_CIPHER_SUITES = "ssl-ciphers";

    public static void main(String args[])
    {
        LoaderOptions options = LoaderOptions.parseArgs(args);
        OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
        SSTableLoader loader = new SSTableLoader(options.directory, new ExternalClient(options.hosts, options.rpcPort, options.user, options.passwd, options.transportFactory), handler);
        DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(options.throttle);
        StreamResultFuture future = null;
        try
        {
            if (options.noProgress)
                future = loader.stream(options.ignores);
            else
                future = loader.stream(options.ignores, new ProgressIndicator());
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (e.getCause() != null)
                System.err.println(e.getCause());
            if (options.debug)
                e.printStackTrace(System.err);
            else
                System.err.println("Run with --debug to get full stack trace or --help to get help.");
            System.exit(1);
        }

        handler.output(String.format("Streaming session ID: %s", future.planId));

        try
        {
            future.get();
            System.exit(0); // We need that to stop non daemonized threads
        }
        catch (Exception e)
        {
            System.err.println("Streaming to the following hosts failed:");
            System.err.println(loader.getFailedHosts());
            System.err.println(e);
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    // Return true when everything is at 100%
    static class ProgressIndicator implements StreamEventHandler
    {
        private final Map<InetAddress, SessionInfo> sessionsByHost = new ConcurrentHashMap<>();
        private final Map<InetAddress, Set<ProgressInfo>> progressByHost = new ConcurrentHashMap<>();

        private long start;
        private long lastProgress;
        private long lastTime;

        public ProgressIndicator()
        {
            start = lastTime = System.nanoTime();
        }

        public void onSuccess(StreamState finalState) {}
        public void onFailure(Throwable t) {}

        public void handleStreamEvent(StreamEvent event)
        {
            if (event.eventType == StreamEvent.Type.STREAM_PREPARED)
            {
                SessionInfo session = ((StreamEvent.SessionPreparedEvent) event).session;
                sessionsByHost.put(session.peer, session);
            }
            else if (event.eventType == StreamEvent.Type.FILE_PROGRESS)
            {
                ProgressInfo progressInfo = ((StreamEvent.ProgressEvent) event).progress;

                // update progress
                Set<ProgressInfo> progresses = progressByHost.get(progressInfo.peer);
                if (progresses == null)
                {
                    progresses = Sets.newSetFromMap(new ConcurrentHashMap<ProgressInfo, Boolean>());
                    progressByHost.put(progressInfo.peer, progresses);
                }
                if (progresses.contains(progressInfo))
                    progresses.remove(progressInfo);
                progresses.add(progressInfo);

                StringBuilder sb = new StringBuilder();
                sb.append("\rprogress: ");

                long totalProgress = 0;
                long totalSize = 0;
                for (Map.Entry<InetAddress, Set<ProgressInfo>> entry : progressByHost.entrySet())
                {
                    SessionInfo session = sessionsByHost.get(entry.getKey());

                    long size = session.getTotalSizeToSend();
                    long current = 0;
                    int completed = 0;
                    for (ProgressInfo progress : entry.getValue())
                    {
                        if (progress.currentBytes == progress.totalBytes)
                            completed++;
                        current += progress.currentBytes;
                    }
                    totalProgress += current;
                    totalSize += size;
                    sb.append("[").append(entry.getKey());
                    sb.append(" ").append(completed).append("/").append(session.getTotalFilesToSend());
                    sb.append(" (").append(size == 0 ? 100L : current * 100L / size).append("%)] ");
                }
                long time = System.nanoTime();
                long deltaTime = TimeUnit.NANOSECONDS.toMillis(time - lastTime);
                lastTime = time;
                long deltaProgress = totalProgress - lastProgress;
                lastProgress = totalProgress;

                sb.append("[total: ").append(totalSize == 0 ? 100L : totalProgress * 100L / totalSize).append("% - ");
                sb.append(mbPerSec(deltaProgress, deltaTime)).append("MB/s");
                sb.append(" (avg: ").append(mbPerSec(totalProgress, TimeUnit.NANOSECONDS.toMillis(time - start))).append("MB/s)]");

                System.out.print(sb.toString());
            }
        }

        private int mbPerSec(long bytes, long timeInMs)
        {
            double bytesPerMs = ((double)bytes) / timeInMs;
            return (int)((bytesPerMs * 1000) / (1024 * 2024));
        }
    }

    static class ExternalClient extends SSTableLoader.Client
    {
        private final Map<String, CFMetaData> knownCfs = new HashMap<>();
        private final Set<InetAddress> hosts;
        private final int rpcPort;
        private final String user;
        private final String passwd;
        private final ITransportFactory transportFactory;

        public ExternalClient(Set<InetAddress> hosts, int port, String user, String passwd, ITransportFactory transportFactory)
        {
            super();
            this.hosts = hosts;
            this.rpcPort = port;
            this.user = user;
            this.passwd = passwd;
            this.transportFactory = transportFactory;
        }

        public void init(String keyspace)
        {
            Iterator<InetAddress> hostiter = hosts.iterator();
            while (hostiter.hasNext())
            {
                try
                {
                    // Query endpoint to ranges map and schemas from thrift
                    InetAddress host = hostiter.next();
                    Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort, this.user, this.passwd, this.transportFactory);

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : client.describe_ring(keyspace))
                    {
                        Range<Token> range = new Range<>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints)
                        {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }

                    String cfQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s'",
                                                 Keyspace.SYSTEM_KS,
                                                 SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                                                 keyspace);
                    CqlResult cfRes = client.execute_cql3_query(ByteBufferUtil.bytes(cfQuery), Compression.NONE, ConsistencyLevel.ONE);


                    for (CqlRow row : cfRes.rows)
                    {
                        String columnFamily = UTF8Type.instance.getString(row.columns.get(1).bufferForName());
                        String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                                                            Keyspace.SYSTEM_KS,
                                                            SystemKeyspace.SCHEMA_COLUMNS_CF,
                                                            keyspace,
                                                            columnFamily);
                        CqlResult columnsRes = client.execute_cql3_query(ByteBufferUtil.bytes(columnsQuery), Compression.NONE, ConsistencyLevel.ONE);

                        CFMetaData metadata = CFMetaData.fromThriftCqlRow(row, columnsRes);
                        knownCfs.put(metadata.cfName, metadata);
                    }
                    break;
                }
                catch (Exception e)
                {
                    if (!hostiter.hasNext())
                        throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
                }
            }
        }

        public CFMetaData getCFMetaData(String keyspace, String cfName)
        {
            return knownCfs.get(cfName);
        }

        private static Cassandra.Client createThriftClient(String host, int port, String user, String passwd, ITransportFactory transportFactory) throws Exception
        {
            TTransport trans = transportFactory.openTransport(host, port);
            TProtocol protocol = new TBinaryProtocol(trans);
            Cassandra.Client client = new Cassandra.Client(protocol);
            if (user != null && passwd != null)
            {
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put(IAuthenticator.USERNAME_KEY, user);
                credentials.put(IAuthenticator.PASSWORD_KEY, passwd);
                AuthenticationRequest authenticationRequest = new AuthenticationRequest(credentials);
                client.login(authenticationRequest);
            }
            return client;
        }
    }

    static class LoaderOptions
    {
        public final File directory;

        public boolean debug;
        public boolean verbose;
        public boolean noProgress;
        public int rpcPort = 9160;
        public String user;
        public String passwd;
        public int throttle = 0;
        public ITransportFactory transportFactory = new TFramedTransportFactory();
        public EncryptionOptions encOptions = new EncryptionOptions.ClientEncryptionOptions();

        public final Set<InetAddress> hosts = new HashSet<InetAddress>();
        public final Set<InetAddress> ignores = new HashSet<InetAddress>();

        LoaderOptions(File directory)
        {
            this.directory = directory;
        }

        public static LoaderOptions parseArgs(String cmdArgs[])
        {
            CommandLineParser parser = new GnuParser();
            CmdLineOptions options = getCmdLineOptions();
            try
            {
                CommandLine cmd = parser.parse(options, cmdArgs, false);

                if (cmd.hasOption(HELP_OPTION))
                {
                    printUsage(options);
                    System.exit(0);
                }

                String[] args = cmd.getArgs();
                if (args.length == 0)
                {
                    System.err.println("Missing sstable directory argument");
                    printUsage(options);
                    System.exit(1);
                }

                if (args.length > 1)
                {
                    System.err.println("Too many arguments");
                    printUsage(options);
                    System.exit(1);
                }

                String dirname = args[0];
                File dir = new File(dirname);

                if (!dir.exists())
                    errorMsg("Unknown directory: " + dirname, options);

                if (!dir.isDirectory())
                    errorMsg(dirname + " is not a directory", options);

                LoaderOptions opts = new LoaderOptions(dir);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.noProgress = cmd.hasOption(NOPROGRESS_OPTION);

                if (cmd.hasOption(THROTTLE_MBITS))
                    opts.throttle = Integer.parseInt(cmd.getOptionValue(THROTTLE_MBITS));

                if (cmd.hasOption(RPC_PORT_OPTION))
                    opts.rpcPort = Integer.parseInt(cmd.getOptionValue(RPC_PORT_OPTION));

                if (cmd.hasOption(USER_OPTION))
                    opts.user = cmd.getOptionValue(USER_OPTION);

                if (cmd.hasOption(PASSWD_OPTION))
                    opts.passwd = cmd.getOptionValue(PASSWD_OPTION);

                if (cmd.hasOption(INITIAL_HOST_ADDRESS_OPTION))
                {
                    String[] nodes = cmd.getOptionValue(INITIAL_HOST_ADDRESS_OPTION).split(",");
                    try
                    {
                        for (String node : nodes)
                        {
                            opts.hosts.add(InetAddress.getByName(node.trim()));
                        }
                    }
                    catch (UnknownHostException e)
                    {
                        errorMsg("Unknown host: " + e.getMessage(), options);
                    }

                }
                else
                {
                    System.err.println("Initial hosts must be specified (-d)");
                    printUsage(options);
                    System.exit(1);
                }

                if (cmd.hasOption(IGNORE_NODES_OPTION))
                {
                    String[] nodes = cmd.getOptionValue(IGNORE_NODES_OPTION).split(",");
                    try
                    {
                        for (String node : nodes)
                        {
                            opts.ignores.add(InetAddress.getByName(node.trim()));
                        }
                    }
                    catch (UnknownHostException e)
                    {
                        errorMsg("Unknown host: " + e.getMessage(), options);
                    }
                }

                if(cmd.hasOption(SSL_TRUSTSTORE))
                {
                    opts.encOptions.truststore = cmd.getOptionValue(SSL_TRUSTSTORE);
                }

                if(cmd.hasOption(SSL_TRUSTSTORE_PW))
                {
                    opts.encOptions.truststore_password = cmd.getOptionValue(SSL_TRUSTSTORE_PW);
                }

                if(cmd.hasOption(SSL_KEYSTORE))
                {
                    opts.encOptions.keystore = cmd.getOptionValue(SSL_KEYSTORE);
                    // if a keystore was provided, lets assume we'll need to use it
                    opts.encOptions.require_client_auth = true;
                }

                if(cmd.hasOption(SSL_KEYSTORE_PW))
                {
                    opts.encOptions.keystore_password = cmd.getOptionValue(SSL_KEYSTORE_PW);
                }

                if(cmd.hasOption(SSL_PROTOCOL))
                {
                    opts.encOptions.protocol = cmd.getOptionValue(SSL_PROTOCOL);
                }

                if(cmd.hasOption(SSL_ALGORITHM))
                {
                    opts.encOptions.algorithm = cmd.getOptionValue(SSL_ALGORITHM);
                }

                if(cmd.hasOption(SSL_STORE_TYPE))
                {
                    opts.encOptions.store_type = cmd.getOptionValue(SSL_STORE_TYPE);
                }

                if(cmd.hasOption(SSL_CIPHER_SUITES))
                {
                    opts.encOptions.cipher_suites = cmd.getOptionValue(SSL_CIPHER_SUITES).split(",");
                }

                if (cmd.hasOption(TRANSPORT_FACTORY))
                {
                    ITransportFactory transportFactory = getTransportFactory(cmd.getOptionValue(TRANSPORT_FACTORY));
                    configureTransportFactory(transportFactory, opts);
                    opts.transportFactory = transportFactory;
                }

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        private static ITransportFactory getTransportFactory(String transportFactory)
        {
            try
            {
                Class<?> factory = Class.forName(transportFactory);
                if (!ITransportFactory.class.isAssignableFrom(factory))
                    throw new IllegalArgumentException(String.format("transport factory '%s' " +
                            "not derived from ITransportFactory", transportFactory));
                return (ITransportFactory) factory.newInstance();
            }
            catch (Exception e)
            {
                throw new IllegalArgumentException(String.format("Cannot create a transport factory '%s'.", transportFactory), e);
            }
        }

        private static void configureTransportFactory(ITransportFactory transportFactory, LoaderOptions opts)
        {
            Map<String, String> options = new HashMap<>();
            // If the supplied factory supports the same set of options as our SSL impl, set those 
            if (transportFactory.supportedOptions().contains(SSLTransportFactory.TRUSTSTORE))
                options.put(SSLTransportFactory.TRUSTSTORE, opts.encOptions.truststore);
            if (transportFactory.supportedOptions().contains(SSLTransportFactory.TRUSTSTORE_PASSWORD))
                options.put(SSLTransportFactory.TRUSTSTORE_PASSWORD, opts.encOptions.truststore_password);
            if (transportFactory.supportedOptions().contains(SSLTransportFactory.PROTOCOL))
                options.put(SSLTransportFactory.PROTOCOL, opts.encOptions.protocol);
            if (transportFactory.supportedOptions().contains(SSLTransportFactory.CIPHER_SUITES))
                options.put(SSLTransportFactory.CIPHER_SUITES, Joiner.on(',').join(opts.encOptions.cipher_suites));

            if (transportFactory.supportedOptions().contains(SSLTransportFactory.KEYSTORE)
                    && opts.encOptions.require_client_auth)
                options.put(SSLTransportFactory.KEYSTORE, opts.encOptions.keystore);
            if (transportFactory.supportedOptions().contains(SSLTransportFactory.KEYSTORE_PASSWORD)
                    && opts.encOptions.require_client_auth)
                options.put(SSLTransportFactory.KEYSTORE_PASSWORD, opts.encOptions.keystore_password);

            // Now check if any of the factory's supported options are set as system properties
            for (String optionKey : transportFactory.supportedOptions())
                if (System.getProperty(optionKey) != null)
                    options.put(optionKey, System.getProperty(optionKey));

            transportFactory.setOptions(options);
        }

        private static void errorMsg(String msg, CmdLineOptions options)
        {
            System.err.println(msg);
            printUsage(options);
            System.exit(1);
        }
        private static CmdLineOptions getCmdLineOptions()
        {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption(null, DEBUG_OPTION,        "display stack traces");
            options.addOption("v",  VERBOSE_OPTION,      "verbose output");
            options.addOption("h",  HELP_OPTION,         "display this help message");
            options.addOption(null, NOPROGRESS_OPTION,   "don't display progress");
            options.addOption("i",  IGNORE_NODES_OPTION, "NODES", "don't stream to this (comma separated) list of nodes");
            options.addOption("d",  INITIAL_HOST_ADDRESS_OPTION, "initial hosts", "try to connect to these hosts (comma separated) initially for ring information");
            options.addOption("p",  RPC_PORT_OPTION, "rpc port", "port used for rpc (default 9160)");
            options.addOption("t",  THROTTLE_MBITS, "throttle", "throttle speed in Mbits (default unlimited)");
            options.addOption("u",  USER_OPTION, "username", "username for cassandra authentication");
            options.addOption("pw", PASSWD_OPTION, "password", "password for cassandra authentication");
            options.addOption("tf", TRANSPORT_FACTORY, "transport factory", "Fully-qualified ITransportFactory class name for creating a connection to cassandra");
            // ssl connection-related options
            options.addOption("ts", SSL_TRUSTSTORE, "TRUSTSTORE", "SSL: full path to truststore");
            options.addOption("tspw", SSL_TRUSTSTORE_PW, "TRUSTSTORE-PASSWORD", "SSL: password of the truststore");
            options.addOption("ks", SSL_KEYSTORE, "KEYSTORE", "SSL: full path to keystore");
            options.addOption("kspw", SSL_KEYSTORE_PW, "KEYSTORE-PASSWORD", "SSL: password of the keystore");
            options.addOption("prtcl", SSL_PROTOCOL, "PROTOCOL", "SSL: connections protocol to use (default: TLS)");
            options.addOption("alg", SSL_ALGORITHM, "ALGORITHM", "SSL: algorithm (default: SunX509)");
            options.addOption("st", SSL_STORE_TYPE, "STORE-TYPE", "SSL: type of store");
            options.addOption("ciphers", SSL_CIPHER_SUITES, "CIPHER-SUITES", "SSL: comma-separated list of encryption suites to use");
            return options;
        }

        public static void printUsage(Options options)
        {
            String usage = String.format("%s [options] <dir_path>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Bulk load the sstables found in the directory <dir_path> to the configured cluster." );
            header.append("The parent directory of <dir_path> is used as the keyspace name. ");
            header.append("So for instance, to load an sstable named Standard1-g-1-Data.db into keyspace Keyspace1, ");
            header.append("you will need to have the files Standard1-g-1-Data.db and Standard1-g-1-Index.db in a ");
            header.append("directory Keyspace1/Standard1/ in the directory and call: sstableloader Keyspace1/Standard1");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }

    public static class CmdLineOptions extends Options
    {
        /**
         * Add option with argument and argument name
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param argName argument name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String argName, String description)
        {
            Option option = new Option(opt, longOpt, true, description);
            option.setArgName(argName);

            return addOption(option);
        }

        /**
         * Add option without argument
         * @param opt shortcut for option name
         * @param longOpt complete option name
         * @param description description of the option
         * @return updated Options object
         */
        public Options addOption(String opt, String longOpt, String description)
        {
            return addOption(new Option(opt, longOpt, false, description));
        }
    }
}
