/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file * distributed with this work for additional information
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

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Upgrader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneUpgrader
{
    private static final String TOOL_NAME = "sstableupgrade";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String KEEP_SOURCE = "keep-source";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        Util.initDatabaseDescriptor();

        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            if (Schema.instance.getCFMetaData(options.keyspace, options.cf) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                                                                 options.keyspace,
                                                                 options.cf));

            Keyspace keyspace = Keyspace.openWithoutSSTables(options.keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(options.cf);

            OutputHandler handler = new OutputHandler.SystemOutput(false, options.debug);
            Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW);
            if (options.snapshot != null)
                lister.onlyBackups(true).snapshots(options.snapshot);
            else
                lister.includeBackups(false);

            Collection<SSTableReader> readers = new ArrayList<>();

            // Upgrade sstables
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Set<Component> components = entry.getValue();
                if (!components.contains(Component.DATA) || !components.contains(Component.PRIMARY_INDEX))
                    continue;

                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs);
                    if (sstable.descriptor.version.equals(SSTableFormat.Type.current().info.getLatestVersion()))
                    {
                        sstable.selfRef().release();
                        continue;
                    }
                    readers.add(sstable);
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }

            int numSSTables = readers.size();
            handler.output("Found " + numSSTables + " sstables that need upgrading.");

            for (SSTableReader sstable : readers)
            {
                try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.UPGRADE_SSTABLES, sstable))
                {
                    Upgrader upgrader = new Upgrader(cfs, txn, handler);
                    upgrader.upgrade(options.keepSource);
                }
                catch (Exception e)
                {
                    System.err.println(String.format("Error upgrading %s: %s", sstable, e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
                finally
                {
                    // we should have released this through commit of the LifecycleTransaction,
                    // but in case the upgrade failed (or something else went wrong) make sure we don't retain a reference
                    sstable.selfRef().ensureReleased();
                }
            }
            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
            LifecycleTransaction.waitForDeletions();
            System.exit(0);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (options.debug)
                e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static class Options
    {
        public final String keyspace;
        public final String cf;
        public final String snapshot;

        public boolean debug;
        public boolean keepSource;

        private Options(String keyspace, String cf, String snapshot)
        {
            this.keyspace = keyspace;
            this.cf = cf;
            this.snapshot = snapshot;
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
                if (args.length >= 4 || args.length < 2)
                {
                    String msg = args.length < 2 ? "Missing arguments" : "Too many arguments";
                    errorMsg(msg, options);
                    System.exit(1);
                }

                String keyspace = args[0];
                String cf = args[1];
                String snapshot = null;
                if (args.length == 3)
                    snapshot = args[2];

                Options opts = new Options(keyspace, cf, snapshot);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.keepSource = cmd.hasOption(KEEP_SOURCE);

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
            options.addOption("h",  HELP_OPTION,           "display this help message");
            options.addOption("k",  KEEP_SOURCE,           "do not delete the source sstables");
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <cf> [snapshot]", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Upgrade the sstables in the given cf (or snapshot) to the current version of Cassandra." );
            header.append("This operation will rewrite the sstables in the specified cf to match the " );
            header.append("currently installed version of Cassandra.\n");
            header.append("The snapshot option will only upgrade the specified snapshot. Upgrading " );
            header.append("snapshots is required before attempting to restore a snapshot taken in a " );
            header.append("major version older than the major version Cassandra is currently running. " );
            header.append("This will replace the files in the given snapshot as well as break any " );
            header.append("hard links to live sstables." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}

