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

import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.OutputHandler;
import static org.apache.cassandra.tools.BulkLoader.CmdLineOptions;

public class StandaloneScrubber
{
    static
    {
        CassandraDaemon.initLog4j();
    }

    private static final String TOOL_NAME = "sstablescrub";
    private static final String VERBOSE_OPTION  = "verbose";
    private static final String DEBUG_OPTION  = "debug";
    private static final String HELP_OPTION  = "help";
    private static final String MANIFEST_CHECK_OPTION  = "manifest-check";

    public static void main(String args[]) throws IOException
    {
        Options options = Options.parseArgs(args);
        try
        {
            // load keyspace descriptions.
            DatabaseDescriptor.loadSchemas();

            if (Schema.instance.getCFMetaData(options.tableName, options.cfName) == null)
                throw new IllegalArgumentException(String.format("Unknown keyspace/columnFamily %s.%s",
                                                                 options.tableName,
                                                                 options.cfName));

            // Do not load sstables since they might be broken
            Table table = Table.openWithoutSSTables(options.tableName);
            ColumnFamilyStore cfs = table.getColumnFamilyStore(options.cfName);
            String snapshotName = "pre-scrub-" + System.currentTimeMillis();

            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            Directories.SSTableLister lister = cfs.directories.sstableLister().skipTemporary(true);

            List<SSTableReader> sstables = new ArrayList<SSTableReader>();

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
                    System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
            }
            System.out.println(String.format("Pre-scrub sstables snapshotted into snapshot %s", snapshotName));

            // If leveled, load the manifest
            LeveledManifest manifest = null;
            if (cfs.directories.tryGetLeveledManifest() != null)
            {
                cfs.directories.snapshotLeveledManifest(snapshotName);
                System.out.println(String.format("Leveled manifest snapshotted into snapshot %s", snapshotName));

                int maxSizeInMB = (int)((((LeveledCompactionStrategy)cfs.getCompactionStrategy()).getMaxSSTableSize()) / (1024L * 1024L));
                manifest = LeveledManifest.create(cfs, maxSizeInMB, sstables);
            }

            if (!options.manifestCheckOnly)
            {
                for (SSTableReader sstable : sstables)
                {
                    try
                    {
                        Scrubber scrubber = new Scrubber(cfs, sstable, handler, true);
                        try
                        {
                            scrubber.scrub();
                        }
                        finally
                        {
                            scrubber.close();
                        }

                        if (manifest != null)
                        {
                            if (scrubber.getNewInOrderSSTable() != null)
                                manifest.add(scrubber.getNewInOrderSSTable());

                            List<SSTableReader> added = scrubber.getNewSSTable() == null
                                ? Collections.<SSTableReader>emptyList()
                                : Collections.<SSTableReader>singletonList(scrubber.getNewSSTable());
                            manifest.replace(Collections.singletonList(sstable), added);
                        }

                        // Remove the sstable (it's been copied by scrub and snapshotted)
                        sstable.markCompacted();
                        sstable.releaseReference();
                    }
                    catch (Exception e)
                    {
                        System.err.println(String.format("Error scrubbing %s: %s", sstable, e.getMessage()));
                        if (options.debug)
                            e.printStackTrace(System.err);
                    }
                }
            }

            // Check (and repair) manifest
            if (manifest != null)
                checkManifest(manifest);

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

    private static void checkManifest(LeveledManifest manifest)
    {
        System.out.println(String.format("Checking leveled manifest"));
        for (int i = 1; i <= manifest.getLevelCount(); ++i)
        {
            List<SSTableReader> sstables = new ArrayList<SSTableReader>(manifest.getLevel(i));
            Collections.sort(sstables, SSTable.sstableComparator);
            if (sstables.isEmpty())
                continue;

            Iterator<SSTableReader> iter = sstables.iterator();
            SSTableReader previous = iter.next();
            while (iter.hasNext())
            {
                SSTableReader current = iter.next();

                if (previous.last.compareTo(current.first) >= 0)
                {
                    System.err.println(String.format("At level %d, %s [%s, %s] overlaps %s [%s, %s]", i,
                                                     previous, previous.first, previous.last,
                                                     current, current.first, current.last));
                    System.out.println(String.format("Sending %s back to L0 to fix intra-level overlapping", current));
                    manifest.sendBackToL0(current);
                }
                else
                {
                    previous = current;
                }
            }
        }
    }

    private static class Options
    {
        public final String tableName;
        public final String cfName;

        public boolean debug;
        public boolean verbose;
        public boolean manifestCheckOnly;

        private Options(String tableName, String cfName)
        {
            this.tableName = tableName;
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

                String tableName = args[0];
                String cfName = args[1];

                Options opts = new Options(tableName, cfName);

                opts.debug = cmd.hasOption(DEBUG_OPTION);
                opts.verbose = cmd.hasOption(VERBOSE_OPTION);
                opts.manifestCheckOnly = cmd.hasOption(MANIFEST_CHECK_OPTION);

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
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <column_family>", TOOL_NAME);
            StringBuilder header = new StringBuilder();
            header.append("--\n");
            header.append("Scrub the sstable for the provided column family." );
            header.append("\n--\n");
            header.append("Options are:");
            new HelpFormatter().printHelp(usage, header.toString(), options, "");
        }
    }
}
