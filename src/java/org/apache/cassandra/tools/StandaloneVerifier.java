/*
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneVerifier
{
    private static final String TOOL_NAME = "sstableverify";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String EXTENDED_OPTION = "extended";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String CHECK_VERSION = "check_version";
    private static final String MUTATE_REPAIR_STATUS = "mutate_repair_status";
    private static final String QUICK = "quick";
    private static final String FORCE = "force";
    private static final String TOKEN_RANGE = "token_range";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        if (!options.force)
        {
            System.err.println("verify will not run without -f or --force. See CASSANDRA-17017 for details.");
            Options.printUsage(Options.getCmdLineOptions());
            System.exit(1);
        }
        initDatabaseDescriptorForTool();

        System.out.println("sstableverify using the following options: " + options);

        List<SSTableReader> sstables = new ArrayList<>();
        int exitCode = 0;
        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk();

            boolean hasFailed = false;

            if (Schema.instance.getTableMetadataRef(options.keyspaceName, options.cfName) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspaceName,
                                                                 options.cfName));

            // Do not load sstables since they might be broken
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cfName);

            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);

            // Verify sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.containsAll(entry.getKey().getFormat().primaryComponents()))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
                    sstables.add(sstable);
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                    exitCode = 1;
                    return;
                }
            }
            IVerifier.Options verifyOptions = IVerifier.options().invokeDiskFailurePolicy(false)
                                                       .extendedVerification(options.extended)
                                                       .checkVersion(options.checkVersion)
                                                       .mutateRepairStatus(options.mutateRepairStatus)
                                                       .checkOwnsTokens(!options.tokens.isEmpty())
                                                       .tokenLookup(ignore -> options.tokens)
                                                       .build();
            handler.output("Running verifier with the following options: " + verifyOptions);
            for (SSTableReader sstable : sstables)
            {
                try (IVerifier verifier = sstable.getVerifier(cfs, handler, true, verifyOptions))
                {
                    verifier.verify();
                }
                catch (Exception e)
                {
                    handler.warn(e, String.format("Error verifying %s: %s", sstable, e.getMessage()));
                    hasFailed = true;
                }
            }

            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);

            for (SSTableReader reader : sstables)
                Throwables.perform((Throwable) null, () -> reader.selfRef().close());

            exitCode = hasFailed ? 1 : 0; // We need that to stop non daemonized threads
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            exitCode = 1;
        }
        finally
        {
            for (SSTableReader reader : sstables)
                Throwables.perform((Throwable) null, () -> reader.selfRef().close());

            System.exit(exitCode);
        }
    }

    private static void initDatabaseDescriptorForTool() {
        if (TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.getBoolean())
            DatabaseDescriptor.toolInitialization(false); //Necessary for testing
        else
            Util.initDatabaseDescriptor();
    }

    private static class Options
    {
        public final String keyspaceName;
        public final String cfName;

        public boolean debug;
        public boolean verbose;
        public boolean extended;
        public boolean checkVersion;
        public boolean mutateRepairStatus;
        public boolean quick;
        public boolean force;
        public Collection<Range<Token>> tokens;

        private Options(String keyspaceName, String cfName)
        {
            this.keyspaceName = keyspaceName;
            this.cfName = cfName;
        }

        public static Options parseArgs(String cmdArgs[])
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
                if (args.length != 2)
                {
                    String prefix = args.length < 2 ? "Missing" : "Too many";
                    System.err.println(prefix + " arguments");
                    printUsage(options);
                    System.exit(1);
                }

                String keyspaceName = args[0];
                String cfName = args[1];

                Options opts = new Options(keyspaceName, cfName);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.extended = cmd.hasOption(EXTENDED_OPTION);
                opts.checkVersion = cmd.hasOption(CHECK_VERSION);
                opts.mutateRepairStatus = cmd.hasOption(MUTATE_REPAIR_STATUS);
                opts.quick = cmd.hasOption(QUICK);
                opts.force = cmd.hasOption(FORCE);

                if (cmd.hasOption(TOKEN_RANGE))
                {
                    opts.tokens = Stream.of(cmd.getOptionValues(TOKEN_RANGE))
                                        .map(StandaloneVerifier::parseTokenRange)
                                        .collect(Collectors.toSet());
                }
                else
                {
                    opts.tokens = Collections.emptyList();
                }

                return opts;
            }
            catch (ParseException e)
            {
                errorMsg(e.getMessage(), options);
                return null;
            }
        }

        public String toString()
        {
            return "Options{" +
                   "keyspaceName='" + keyspaceName + '\'' +
                   ", cfName='" + cfName + '\'' +
                   ", debug=" + debug +
                   ", verbose=" + verbose +
                   ", extended=" + extended +
                   ", checkVersion=" + checkVersion +
                   ", mutateRepairStatus=" + mutateRepairStatus +
                   ", quick=" + quick +
                   ", tokens=" + tokens +
                   '}';
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
            options.addOption(null, DEBUG_OPTION,          "display stack traces");
            options.addOption("e",  EXTENDED_OPTION,       "extended verification");
            options.addOption("v",  VERBOSE_OPTION,        "verbose output");
            options.addOption("h",  HELP_OPTION,           "display this help message");
            options.addOption("c",  CHECK_VERSION,         "make sure sstables are the latest version");
            options.addOption("r",  MUTATE_REPAIR_STATUS,  "don't mutate repair status");
            options.addOption("q",  QUICK,                 "do a quick check, don't read all data");
            options.addOption("f",  FORCE,                 "force verify - see CASSANDRA-17017");
            options.addOptionList("t", TOKEN_RANGE, "range", "long token range of the format left,right. This may be provided multiple times to define multiple different ranges");
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <column_family> force", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Verify the sstable for the provided table." );
            header.append("\n--\n");
            header.append("NOTE: There are significant risks associated with using this tool; it likely doesn't do what " +
                          "you expect and there are known edge cases. You must provide a -f or --force argument in " +
                          "order to allow usage of the tool -> see CASSANDRA-9947 and CASSANDRA-17017 for known risks.\n");
            header.append("https://issues.apache.org/jira/browse/CASSANDRA-9947\n");
            header.append("https://issues.apache.org/jira/browse/CASSANDRA-17017");
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }

    private static Range<Token> parseTokenRange(String line)
    {
        String[] split = line.split(",");
        if (split.length != 2)
            throw new IllegalArgumentException("Unable to parse token range from " + line + "; format is left,right but saw " + split.length + " parts");
        long left = Long.parseLong(split[0]);
        long right = Long.parseLong(split[1]);
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }
}
