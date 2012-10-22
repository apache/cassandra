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
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.commons.cli.*;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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

    public static void main(String args[]) throws IOException
    {
        LoaderOptions options = LoaderOptions.parseArgs(args);
        try
        {
            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            SSTableLoader loader = new SSTableLoader(options.directory, new ExternalClient(handler, options.hosts, options.rpcPort, options.user, options.passwd), handler);
            DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(options.throttle);
            SSTableLoader.LoaderFuture future = loader.stream(options.ignores);

            if (options.noProgress)
            {
                future.get();
            }
            else
            {
                ProgressIndicator indicator = new ProgressIndicator(future.getPendingFiles());
                indicator.start();
                System.out.println("");
                boolean printEnd = false;
                while (!future.isDone())
                {
                    if (indicator.printProgress())
                    {
                        // We're done with streaming
                        System.out.println("\nWaiting for targets to rebuild indexes ...");
                        printEnd = true;
                        future.get();
                        assert future.isDone();
                    }
                    else
                    {
                        try { Thread.sleep(1000L); } catch (Exception e) {}
                    }
                }
                if (!printEnd)
                    indicator.printProgress();
                if (future.hadFailures())
                {
                    System.err.println("Streaming to the following hosts failed:");
                    System.err.println(future.getFailedHosts());
                    System.exit(1);
                }
            }

            System.exit(0); // We need that to stop non daemonized threads
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    // Return true when everything is at 100%
    static class ProgressIndicator
    {
        private final Map<InetAddress, Collection<PendingFile>> filesByHost;
        private long startTime;
        private long lastProgress;
        private long lastTime;

        public ProgressIndicator(Map<InetAddress, Collection<PendingFile>> filesByHost)
        {
            this.filesByHost = new HashMap<InetAddress, Collection<PendingFile>>(filesByHost);
        }

        public void start()
        {
            startTime = System.currentTimeMillis();
        }

        public boolean printProgress()
        {
            boolean done = true;
            StringBuilder sb = new StringBuilder();
            sb.append("\rprogress: ");
            long totalProgress = 0;
            long totalSize = 0;
            for (Map.Entry<InetAddress, Collection<PendingFile>> entry : filesByHost.entrySet())
            {
                long progress = 0;
                long size = 0;
                int completed = 0;
                Collection<PendingFile> pendings = entry.getValue();
                for (PendingFile f : pendings)
                {
                    progress += f.progress;
                    size += f.size;
                    if (f.progress == f.size)
                        completed++;
                }
                totalProgress += progress;
                totalSize += size;
                if (completed != pendings.size())
                    done = false;
                sb.append("[").append(entry.getKey());
                sb.append(" ").append(completed).append("/").append(pendings.size());
                sb.append(" (").append(size == 0 ? 100L : progress * 100L / size).append(")] ");
            }
            long time = System.currentTimeMillis();
            long deltaTime = time - lastTime;
            lastTime = time;
            long deltaProgress = totalProgress - lastProgress;
            lastProgress = totalProgress;

            sb.append("[total: ").append(totalSize == 0 ? 100L : totalProgress * 100L / totalSize).append(" - ");
            sb.append(mbPerSec(deltaProgress, deltaTime)).append("MB/s");
            sb.append(" (avg: ").append(mbPerSec(totalProgress, time - startTime)).append("MB/s)]");
            System.out.print(sb.toString());
            return done;
        }

        private int mbPerSec(long bytes, long timeInMs)
        {
            double bytesPerMs = ((double)bytes) / timeInMs;
            return (int)((bytesPerMs * 1000) / (1024 * 2024));
        }
    }

    static class ExternalClient extends SSTableLoader.Client
    {
        private final Set<String> knownCfs = new HashSet<String>();
        private final Set<InetAddress> hosts;
        private final int rpcPort;
        private final String user;
        private final String passwd;

        public ExternalClient(OutputHandler outputHandler, Set<InetAddress> hosts, int port, String user, String passwd)
        {
            super();
            this.hosts = hosts;
            this.rpcPort = port;
            this.user = user;
            this.passwd = passwd;
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
                    Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort, this.user, this.passwd);

                    setPartitioner(client.describe_partitioner());
                    Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                    for (TokenRange tr : client.describe_ring(keyspace))
                    {
                        Range<Token> range = new Range<Token>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints)
                        {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }

                    String query = String.format("SELECT columnfamily_name FROM %s.%s WHERE keyspace_name = '%s'",
                                                 Table.SYSTEM_KS,
                                                 SystemTable.SCHEMA_COLUMNFAMILIES_CF,
                                                 keyspace);
                    CqlResult result = client.execute_cql3_query(ByteBufferUtil.bytes(query), Compression.NONE, ConsistencyLevel.ONE);
                    for (CqlRow row : result.rows)
                        knownCfs.add(new String(row.getColumns().get(0).getValue(), "UTF8"));
                    break;
                }
                catch (Exception e)
                {
                    if (!hostiter.hasNext())
                        throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
                }
            }
        }

        public boolean validateColumnFamily(String keyspace, String cfName)
        {
            return knownCfs.contains(cfName);
        }

        private static Cassandra.Client createThriftClient(String host, int port, String user, String passwd) throws Exception
        {
            TSocket socket = new TSocket(host, port);
            TTransport trans = new TFramedTransport(socket);
            trans.open();
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

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
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
