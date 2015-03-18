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
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.compaction.WrappingCompactionStrategy;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneScrubber
{
    private static final String TOOL_NAME = "sstablescrub";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String MANIFEST_CHECK_OPTION  = "manifest-check";
    private static final String SKIP_CORRUPTED_OPTION = "skip-corrupted";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            if (Schema.instance.getCFMetaData(options.keyspaceName, options.cfName) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspaceName,
                                                                 options.cfName));

            // Do not load sstables since they might be broken
            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspaceName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cfName);
            String snapshotName = "pre-scrub-" + System.currentTimeMillis();

            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            Directories.SSTableLister lister = cfs.directories.sstableLister().skipTemporary(true);

            List<SSTableReader> sstables = new ArrayList<>();

            // Scrub sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA) || !components.contains(Component.PRIMARY_INDEX))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs.metadata);
                    sstables.add(sstable);

                    File snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                    sstable.createLinks(snapshotDirectory.getPath());

                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }
            System.out.println(String.format("Pre-scrub sstables snapshotted into snapshot %s", snapshotName));

            if (!options.manifestCheckOnly)
            {
                for (SSTableReader sstable : sstables)
                {
                    try
                    {
                        Scrubber scrubber = new Scrubber(cfs, sstable, options.skipCorrupted, handler, true);
                        try
                        {
                            scrubber.scrub();
                        }
                        finally
                        {
                            scrubber.close();
                        }

                        // Remove the sstable (it's been copied by scrub and snapshotted)
                        sstable.markObsolete();
                        sstable.selfRef().release();
                    }
                    catch (Exception e)
                    {
                        System.err.println(String.format("Error scrubbing %s: %s", sstable, e.getMessage()));
                        e.printStackTrace(System.err);
                    }
                }
            }

            // Check (and repair) manifests
            checkManifest(cfs.getCompactionStrategy(), cfs, sstables);
            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
            SSTableDeletingTask.waitForDeletions();
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

    private static void checkManifest(AbstractCompactionStrategy strategy, ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        WrappingCompactionStrategy wrappingStrategy = (WrappingCompactionStrategy)strategy;
        int maxSizeInMB = (int)((cfs.getCompactionStrategy().getMaxSSTableBytes()) / (1024L * 1024L));
        if (wrappingStrategy.getWrappedStrategies().size() == 2 && wrappingStrategy.getWrappedStrategies().get(0) instanceof LeveledCompactionStrategy)
        {
            System.out.println("Checking leveled manifest");
            Predicate<SSTableReader> repairedPredicate = new Predicate<SSTableReader>()
            {
                @Override
                public boolean apply(SSTableReader sstable)
                {
                    return sstable.isRepaired();
                }
            };

            List<SSTableReader> repaired = Lists.newArrayList(Iterables.filter(sstables, repairedPredicate));
            List<SSTableReader> unRepaired = Lists.newArrayList(Iterables.filter(sstables, Predicates.not(repairedPredicate)));

            LeveledManifest repairedManifest = LeveledManifest.create(cfs, maxSizeInMB, repaired);
            for (int i = 1; i < repairedManifest.getLevelCount(); i++)
            {
                repairedManifest.repairOverlappingSSTables(i);
            }
            LeveledManifest unRepairedManifest = LeveledManifest.create(cfs, maxSizeInMB, unRepaired);
            for (int i = 1; i < unRepairedManifest.getLevelCount(); i++)
            {
                unRepairedManifest.repairOverlappingSSTables(i);
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
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
