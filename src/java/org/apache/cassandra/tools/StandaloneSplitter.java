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

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.commons.cli.*;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneSplitter
{
    public static final int DEFAULT_SSTABLE_SIZE = 50;

    private static final String TOOL_NAME = "sstablessplit";
    private static final String DEBUG_OPTION = "debug";
    private static final String HELP_OPTION = "help";
    private static final String NO_SNAPSHOT_OPTION = "no-snapshot";
    private static final String SIZE_OPTION = "size";

    public static void main(String args[])
    {
        Options options = Options.parseArgs(args);
        Util.initDatabaseDescriptor();

        try
        {
            // load keyspace descriptions.
            Schema.instance.loadFromDisk(false);

            String ksName = null;
            String cfName = null;
            Map<Descriptor, Set<Component>> parsedFilenames = new HashMap<Descriptor, Set<Component>>();
            for (String filename : options.filenames)
            {
                File file = new File(filename);
                if (!file.exists()) {
                    System.out.println("Skipping inexisting file " + file);
                    continue;
                }

                Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file.getParentFile(), file.getName());
                if (pair == null) {
                    System.out.println("Skipping non sstable file " + file);
                    continue;
                }
                Descriptor desc = pair.left;

                if (ksName == null)
                    ksName = desc.ksname;
                else if (!ksName.equals(desc.ksname))
                    throw new IllegalArgumentException("All sstables must be part of the same keyspace");

                if (cfName == null)
                    cfName = desc.cfname;
                else if (!cfName.equals(desc.cfname))
                    throw new IllegalArgumentException("All sstables must be part of the same table");

                Set<Component> components = new HashSet<Component>(Arrays.asList(new Component[]{
                    Component.DATA,
                    Component.PRIMARY_INDEX,
                    Component.FILTER,
                    Component.COMPRESSION_INFO,
                    Component.STATS
                }));

                Iterator<Component> iter = components.iterator();
                while (iter.hasNext()) {
                    Component component = iter.next();
                    if (!(new File(desc.filenameFor(component)).exists()))
                        iter.remove();
                }
                parsedFilenames.put(desc, components);
            }

            if (ksName == null || cfName == null)
            {
                System.err.println("No valid sstables to split");
                System.exit(1);
            }

            // Do not load sstables since they might be broken
            Keyspace keyspace = Keyspace.openWithoutSSTables(ksName);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
            String snapshotName = "pre-split-" + System.currentTimeMillis();

            List<SSTableReader> sstables = new ArrayList<>();
            for (Map.Entry<Descriptor, Set<Component>> fn : parsedFilenames.entrySet())
            {
                try
                {
                    SSTableReader sstable = SSTableReader.openNoValidation(fn.getKey(), fn.getValue(), cfs);
                    if (!isSSTableLargerEnough(sstable, options.sizeInMB)) {
                        System.out.println(String.format("Skipping %s: it's size (%.3f MB) is less than the split size (%d MB)",
                                sstable.getFilename(), ((sstable.onDiskLength() * 1.0d) / 1024L) / 1024L, options.sizeInMB));
                        continue;
                    }
                    sstables.add(sstable);

                    if (options.snapshot) {
                        File snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                        sstable.createLinks(snapshotDirectory.getPath());
                    }

                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.println(String.format("Error Loading %s: %s", fn.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }
            if (sstables.isEmpty()) {
                System.out.println("No sstables needed splitting.");
                System.exit(0);
            }
            if (options.snapshot)
                System.out.println(String.format("Pre-split sstables snapshotted into snapshot %s", snapshotName));

            for (SSTableReader sstable : sstables)
            {
                try (LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.UNKNOWN, sstable))
                {
                    new SSTableSplitter(cfs, transaction, options.sizeInMB).split();
                }
                catch (Exception e)
                {
                    System.err.println(String.format("Error splitting %s: %s", sstable, e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);

                    sstable.selfRef().release();
                }
            }
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

    /**
     * filter the sstable which size is less than the expected max sstable size.
     */
    private static boolean isSSTableLargerEnough(SSTableReader sstable, int sizeInMB) {
        return sstable.onDiskLength() > sizeInMB * 1024L * 1024L;
    }

    private static class Options
    {
        public final List<String> filenames;

        public boolean debug;
        public boolean snapshot;
        public int sizeInMB;

        private Options(List<String> filenames)
        {
            this.filenames = filenames;
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
                if (args.length == 0)
                {
                    System.err.println("No sstables to split");
                    printUsage(options);
                    System.exit(1);
                }
                Options opts = new Options(Arrays.asList(args));
                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.snapshot = !cmd.hasOption(NO_SNAPSHOT_OPTION);
                opts.sizeInMB = DEFAULT_SSTABLE_SIZE;

                if (cmd.hasOption(SIZE_OPTION))
                    opts.sizeInMB = Integer.parseInt(cmd.getOptionValue(SIZE_OPTION));

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
            options.addOption(null, NO_SNAPSHOT_OPTION,    "don't snapshot the sstables before splitting");
            options.addOption("s",  SIZE_OPTION, "size",   "maximum size in MB for the output sstables (default: " + DEFAULT_SSTABLE_SIZE + ")");
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <filename> [<filename>]*", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Split the provided sstables files in sstables of maximum provided file size (see option --" + SIZE_OPTION + ")." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
