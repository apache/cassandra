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

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableHeaderFix;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tools.BulkLoader.CmdLineOptions;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

public class StandaloneScrubber
{
    public static final String REINSERT_OVERFLOWED_TTL_OPTION_DESCRIPTION = "Rewrites rows with overflowed expiration date affected by CASSANDRA-14092 with " +
                                                                            "the maximum supported expiration date of 2038-01-19T03:14:06+00:00. " +
                                                                            "The rows are rewritten with the original timestamp incremented by one millisecond " +
                                                                            "to override/supersede any potential tombstone that may have been generated " +
                                                                            "during compaction of the affected rows.";

    private static final String TOOL_NAME = "sstablescrub";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String MANIFEST_CHECK_OPTION  = "manifest-check";
    private static final String SKIP_CORRUPTED_OPTION = "skip-corrupted";
    private static final String NO_VALIDATE_OPTION = "no-validate";
    private static final String REINSERT_OVERFLOWED_TTL_OPTION = "reinsert-overflowed-ttl";
    private static final String HEADERFIX_OPTION = "header-fix";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);

        if (Boolean.getBoolean(Util.ALLOW_TOOL_REINIT_FOR_TEST))
            DatabaseDescriptor.toolInitialization(false); //Necessary for testing
        else
            Util.initDatabaseDescriptor();

        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            if (Schema.instance.getKeyspaceMetadata(options.keyspaceName) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace %s", options.keyspaceName));

            // Do not load sstables since they might be broken
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);

            ColumnFamilyStore cfs = null;
            for (ColumnFamilyStore c : keyspace.getValidColumnFamilies(true, false, options.cfName))
            {
                if (c.name.equals(options.cfName))
                {
                    cfs = c;
                    break;
                }
            }

            if (cfs == null)
                throw new IllegalArgumentException(String.format("Unknown table %s.%s",
                                                                  options.keyspaceName,
                                                                  options.cfName));

            String snapshotName = "pre-scrub-" + System.currentTimeMillis();

            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
            List<Pair<Descriptor, Set<Component>>> listResult = new ArrayList<>();

            // create snapshot
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Descriptor descriptor = entry.getKey();
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA))
                    continue;

                listResult.add(Pair.create(descriptor, components));

                File snapshotDirectory = Directories.getSnapshotDirectory(descriptor, snapshotName);
                SSTableReader.createLinks(descriptor, components, snapshotDirectory.getPath());
            }
            System.out.println(String.format("Pre-scrub sstables snapshotted into snapshot %s", snapshotName));

            if (options.headerFixMode != Options.HeaderFixMode.OFF)
            {
                // Run the frozen-UDT checks _before_ the sstables are opened

                List<String> logOutput = new ArrayList<>();

                SSTableHeaderFix.Builder headerFixBuilder = SSTableHeaderFix.builder()
                                                                            .logToList(logOutput)
                                                                            .schemaCallback(() -> Schema.instance::getTableMetadata);
                if (options.headerFixMode == Options.HeaderFixMode.VALIDATE)
                    headerFixBuilder = headerFixBuilder.dryRun();

                for (Pair<Descriptor, Set<Component>> p : listResult)
                    headerFixBuilder.withPath(Paths.get(p.left.filenameFor(Component.DATA)));

                SSTableHeaderFix headerFix = headerFixBuilder.build();
                try
                {
                    headerFix.execute();
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    if (options.debug)
                        e.printStackTrace(System.err);
                }

                if (headerFix.hasChanges() || headerFix.hasError())
                    logOutput.forEach(System.out::println);

                if (headerFix.hasError())
                {
                    System.err.println("Errors in serialization-header detected, aborting.");
                    System.exit(1);
                }

                switch (options.headerFixMode)
                {
                    case VALIDATE_ONLY:
                    case FIX_ONLY:
                        System.out.printf("Not continuing with scrub, since '--%s %s' was specified.%n",
                                          HEADERFIX_OPTION,
                                          options.headerFixMode.asCommandLineOption());
                        System.exit(0);
                    case VALIDATE:
                        if (headerFix.hasChanges())
                        {
                            System.err.printf("Unfixed, but fixable errors in serialization-header detected, aborting. " +
                                              "Use a non-validating mode ('-e %s' or '-e %s') for --%s%n",
                                              Options.HeaderFixMode.FIX.asCommandLineOption(),
                                              Options.HeaderFixMode.FIX_ONLY.asCommandLineOption(),
                                              HEADERFIX_OPTION);
                            System.exit(2);
                        }
                        break;
                    case FIX:
                        break;
                }
            }

            List<SSTableReader> sstables = new ArrayList<>();

            // Open sstables
            for (Pair<Descriptor, Set<Component>> pair : listResult)
            {
                Descriptor descriptor = pair.left;
                Set<Component> components = pair.right;
                if (!components.contains(Component.DATA))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(descriptor, components, cfs);
                    sstables.add(sstable);
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", descriptor, e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }

            if (!options.manifestCheckOnly)
            {
                for (SSTableReader sstable : sstables)
                {
                    try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, sstable))
                    {
                        txn.obsoleteOriginals(); // make sure originals are deleted and avoid NPE if index is missing, CASSANDRA-9591
                        try (Scrubber scrubber = new Scrubber(cfs, txn, options.skipCorrupted, handler, !options.noValidate, options.reinserOverflowedTTL))
                        {
                            scrubber.scrub();
                        }
                        catch (Throwable t)
                        {
                            if (!cfs.rebuildOnFailedScrub(t))
                            {
                                System.out.println(t.getMessage());
                                throw t;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        System.err.println(String.format("Error scrubbing %s: %s", sstable, e.getMessage()));
                        e.printStackTrace(System.err);
                    }
                }
            }

            // Check (and repair) manifests
            checkManifest(cfs.getCompactionStrategyManager(), cfs, sstables);
            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
            LifecycleTransaction.waitForDeletions();
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

    private static void checkManifest(CompactionStrategyManager strategyManager, ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        if (strategyManager.getCompactionParams().klass().equals(LeveledCompactionStrategy.class))
        {
            int maxSizeInMB = (int)((cfs.getCompactionStrategyManager().getMaxSSTableBytes()) / (1024L * 1024L));
            int fanOut = cfs.getCompactionStrategyManager().getLevelFanoutSize();
            for (AbstractStrategyHolder.GroupedSSTableContainer sstableGroup : strategyManager.groupSSTables(sstables))
            {
                for (int i = 0; i < sstableGroup.numGroups(); i++)
                {
                    List<SSTableReader> groupSSTables = new ArrayList<>(sstableGroup.getGroup(i));
                    // creating the manifest makes sure the leveling is sane:
                    LeveledManifest.create(cfs, maxSizeInMB, fanOut, groupSSTables);
                }
            }
        }
    }

    private static class Options
    {
        public final String keyspaceName;
        public final String cfName;

        public boolean debug;
        public boolean verbose;
        public boolean manifestCheckOnly;
        public boolean skipCorrupted;
        public boolean noValidate;
        public boolean reinserOverflowedTTL;
        public HeaderFixMode headerFixMode = HeaderFixMode.VALIDATE;

        enum HeaderFixMode
        {
            VALIDATE_ONLY,
            VALIDATE,
            FIX_ONLY,
            FIX,
            OFF;

            static HeaderFixMode fromCommandLine(String value)
            {
                return valueOf(value.replace('-', '_').toUpperCase().trim());
            }

            String asCommandLineOption()
            {
                return name().toLowerCase().replace('_', '-');
            }
        }

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
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    System.err.println(msg);
                    printUsage(options);
                    System.exit(1);
                }

                String keyspaceName = args[0];
                String cfName = args[1];

                Options opts = new Options(keyspaceName, cfName);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.manifestCheckOnly = cmd.hasOption(MANIFEST_CHECK_OPTION);
                opts.skipCorrupted = cmd.hasOption(SKIP_CORRUPTED_OPTION);
                opts.noValidate = cmd.hasOption(NO_VALIDATE_OPTION);
                opts.reinserOverflowedTTL = cmd.hasOption(REINSERT_OVERFLOWED_TTL_OPTION);
                if (cmd.hasOption(HEADERFIX_OPTION))
                {
                    try
                    {
                        opts.headerFixMode = HeaderFixMode.fromCommandLine(cmd.getOptionValue(HEADERFIX_OPTION));
                    }
                    catch (Exception e)
                    {
                        errorMsg(String.format("Invalid argument value '%s' for --%s", cmd.getOptionValue(HEADERFIX_OPTION), HEADERFIX_OPTION), options);
                        return null;
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
            options.addOption(null, DEBUG_OPTION,          "display stack traces");
            options.addOption("v",  VERBOSE_OPTION,        "verbose output");
            options.addOption("h",  HELP_OPTION,           "display this help message");
            options.addOption("m",  MANIFEST_CHECK_OPTION, "only check and repair the leveled manifest, without actually scrubbing the sstables");
            options.addOption("s",  SKIP_CORRUPTED_OPTION, "skip corrupt rows in counter tables");
            options.addOption("n",  NO_VALIDATE_OPTION,    "do not validate columns using column validator");
            options.addOption("e",  HEADERFIX_OPTION,      true, "Option whether and how to perform a " +
                                                                 "check of the sstable serialization-headers and fix known, " +
                                                                 "fixable issues.\n" +
                                                                 "Possible argument values:\n" +
                                                                 "- validate-only: validate the serialization-headers, " +
                                                                 "but do not fix those. Do not continue with scrub - " +
                                                                 "i.e. only validate the header (dry-run of fix-only).\n" +
                                                                 "- validate: (default) validate the serialization-headers, " +
                                                                 "but do not fix those and only continue with scrub if no " +
                                                                 "error were detected.\n" +
                                                                 "- fix-only: validate and fix the serialization-headers, " +
                                                                 "don't continue with scrub.\n" +
                                                                 "- fix: validate and fix the serialization-headers, do not " +
                                                                 "fix and do not continue with scrub if the serialization-header " +
                                                                 "check encountered errors.\n" +
                                                                 "- off: don't perform the serialization-header checks.");
            options.addOption("r", REINSERT_OVERFLOWED_TTL_OPTION, REINSERT_OVERFLOWED_TTL_OPTION_DESCRIPTION);
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <column_family>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Scrub the sstable for the provided table." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(120, usage, header.toString(), options, "");
        }
    }
}
