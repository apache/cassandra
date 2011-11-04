/**
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
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PendingFile;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.commons.cli.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BulkLoader
{
    private static final String TOOL_NAME = "sstableloader";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String NOPROGRESS_OPTION  = "no-progress";
    private static final String IGNORE_NODES_OPTION  = "ignore";

    public static void main(String args[]) throws IOException
    {
        LoaderOptions options = LoaderOptions.parseArgs(args);
        try
        {
            SSTableLoader loader = new SSTableLoader(options.directory, new ExternalClient(options), options);
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
            sb.append(" (avg: ").append(mbPerSec(totalProgress, time - startTime)).append("MB/s)]");;
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
        private final Map<String, Set<String>> knownCfs = new HashMap<String, Set<String>>();
        private final SSTableLoader.OutputHandler outputHandler;

        public ExternalClient(SSTableLoader.OutputHandler outputHandler)
        {
            super();
            this.outputHandler = outputHandler;
        }

        public void init(String keyspace)
        {
            outputHandler.output(String.format("Starting client (and waiting %d seconds for gossip) ...", StorageService.RING_DELAY / 1000));
            try
            {
                // Init gossip
                StorageService.instance.initClient();

                Set<InetAddress> hosts = Gossiper.instance.getLiveMembers();
                hosts.remove(FBUtilities.getBroadcastAddress());
                if (hosts.isEmpty())
                    throw new IllegalStateException("Cannot load any sstable, no live member found in the cluster");

                // Query endpoint to ranges map and schemas from thrift
                String host = hosts.iterator().next().toString().substring(1);
                int port = DatabaseDescriptor.getRpcPort();

                Cassandra.Client client = createThriftClient(host, port);
                List<TokenRange> tokenRanges = client.describe_ring(keyspace);
                List<KsDef> ksDefs = client.describe_keyspaces();

                Token.TokenFactory tkFactory = StorageService.getPartitioner().getTokenFactory();

                try
                {
                    for (TokenRange tr : tokenRanges)
                    {
                        Range range = new Range(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token));
                        for (String ep : tr.endpoints)
                        {
                            addRangeForEndpoint(range, InetAddress.getByName(ep));
                        }
                    }
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException("Got an unknow host from describe_ring()", e);
                }

                for (KsDef ksDef : ksDefs)
                {
                    Set<String> cfs = new HashSet<String>();
                    for (CfDef cfDef : ksDef.cf_defs)
                        cfs.add(cfDef.name);
                    knownCfs.put(ksDef.name, cfs);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void stop()
        {
            StorageService.instance.stopClient();
        }

        public boolean validateColumnFamily(String keyspace, String cfName)
        {
            Set<String> cfs = knownCfs.get(keyspace);
            return cfs != null && cfs.contains(cfName);
        }

        private static Cassandra.Client createThriftClient(String host, int port) throws TTransportException
        {
            TSocket socket = new TSocket(host, port);
            TTransport trans = new TFramedTransport(socket);
            trans.open();
            TProtocol protocol = new TBinaryProtocol(trans);
            return new Cassandra.Client(protocol);
        }
    }

    static class LoaderOptions implements SSTableLoader.OutputHandler
    {
        public final File directory;

        public boolean debug;
        public boolean verbose;
        public boolean noProgress;

        public Set<InetAddress> ignores = new HashSet<InetAddress>();

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

        public void output(String msg)
        {
            System.out.println(msg);
        }

        public void debug(String msg)
        {
            if (verbose)
                System.out.println(msg);
        }

        private static CmdLineOptions getCmdLineOptions()
        {
            CmdLineOptions options = new CmdLineOptions();
            options.addOption(null, DEBUG_OPTION,        "display stack traces");
            options.addOption("v",  VERBOSE_OPTION,      "verbose output");
            options.addOption("h",  HELP_OPTION,         "display this help message");
            options.addOption(null, NOPROGRESS_OPTION,   "don't display progress");
            options.addOption("i",  IGNORE_NODES_OPTION, "NODES", "don't stream to this (comma separated) list of nodes");
            return options;
        }

        public static void printUsage(Options options)
        {
            String usage = String.format("%s [options] <dir_path>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Bulk load the sstables find in the directory <dir_path> to the configured cluster." );
            header.append("The last directory of <dir_path> is used as the keyspace name. ");
            header.append("So for instance, to load a sstable named Standard1-g-1-Data.db into keyspace Keyspace1, ");
            header.append("you will need to have the files Standard1-g-1-Data.db and Standard1-g-1-Index.db in a ");
            header.append("directory Keyspace1/ in the current directory and call: sstableloader Keyspace1");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }

    private static class CmdLineOptions extends Options
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
