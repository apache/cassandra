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
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

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

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        Util.initDatabaseDescriptor();

        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            if (Schema.instance.getKSMetaData(options.keyspaceName) == null)
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

            List<SSTableReader> sstables = new ArrayList<>();

            // Scrub sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
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

            LeveledManifest repairedManifest = LeveledManifest.create(cfs, maxSizeInMB, cfs.getLevelFanoutSize(), repaired);
            for (int i = 1; i < repairedManifest.getLevelCount(); i++)
            {
                repairedManifest.repairOverlappingSSTables(i);
            }
            LeveledManifest unRepairedManifest = LeveledManifest.create(cfs, maxSizeInMB, cfs.getLevelFanoutSize(), unRepaired);
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
        public boolean noValidate;
        public boolean reinserOverflowedTTL;

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
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
