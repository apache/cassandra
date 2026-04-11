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

import java.io.PrintStream;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * During compaction we can drop entire sstables if they only contain expired tombstones and if it is guaranteed
 * to not cover anything in other sstables. An expired sstable can be blocked from getting dropped if its newest
 * timestamp is newer than the oldest data in another sstable.
 * <p>
 * This class outputs all sstables that are blocking other sstables from getting dropped so that a user can
 * figure out why certain sstables are still on disk.
 */
public class SSTableExpiredBlockers
{
    private static final String TOOL_NAME = "sstableexpiredblockers";

    private static class Options
    {
        private static final String HUMAN_READABLE_OPTION = "human-readable";
        private static final String HELP_OPTION = "help";

        public final String keyspace;
        public final String table;
        public boolean humanReadable = false;

        private Options(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
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

                String keyspace = args[0];
                String table = args[1];

                Options opts = new Options(keyspace, table);
                opts.humanReadable = cmd.hasOption(HUMAN_READABLE_OPTION);

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
            options.addOption("H", "human-readable", "Displays values in a human-readable format");
            return options;
        }

        private static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <keyspace> <table>", TOOL_NAME);
            String header = "--\n" +
                            "Outputs all SSTables that are blocking other SSTables from getting dropped." +
                            "\n--\n" +
                            "Options are:";
            new HelpFormatter().printHelp(120, usage, header, options, "");
        }
    }

    public static void main(String[] args)
    {
        Options options = Options.parseArgs(args);

        PrintStream out = System.out;

        Util.initDatabaseDescriptor();
        ClusterMetadataService.initializeForTools(false);

        String keyspace = options.keyspace;
        String columnfamily = options.table;

        TableMetadata metadata = Schema.instance.validateTable(keyspace, columnfamily);

        Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnfamily);
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
        Set<SSTableReader> sstables = new HashSet<>();
        for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet())
        {
            if (sstable.getKey() != null)
            {
                try
                {
                    SSTableReader reader = SSTableReader.open(cfs, sstable.getKey());
                    sstables.add(reader);
                }
                catch (Throwable t)
                {
                    out.println("Couldn't open sstable: " + sstable.getKey().fileFor(Components.DATA) + " (" + t.getMessage() + ")");
                }
            }
        }
        if (sstables.isEmpty())
        {
            out.printf("No sstables for %s.%s", keyspace, columnfamily);
            System.exit(1);
        }

        long gcBefore = (currentTimeMillis() / 1000) - metadata.params.gcGraceSeconds;
        Multimap<SSTableReader, SSTableReader> blockers = checkForExpiredSSTableBlockers(sstables, gcBefore);
        for (SSTableReader blocker : blockers.keySet())
        {
            out.printf("%s blocks %d expired sstables from getting dropped: %s%n%n",
                       formatForExpiryTracing(options.humanReadable, Collections.singleton(blocker)),
                       blockers.get(blocker).size(),
                       formatForExpiryTracing(options.humanReadable, blockers.get(blocker)));
        }

        System.exit(0);
    }

    public static Multimap<SSTableReader, SSTableReader> checkForExpiredSSTableBlockers(Iterable<SSTableReader> sstables, long gcBefore)
    {
        Multimap<SSTableReader, SSTableReader> blockers = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables)
        {
            if (sstable.getMaxLocalDeletionTime() < gcBefore)
            {
                for (SSTableReader potentialBlocker : sstables)
                {
                    if (!potentialBlocker.equals(sstable) &&
                        potentialBlocker.getMinTimestamp() <= sstable.getMaxTimestamp() &&
                        potentialBlocker.getMaxLocalDeletionTime() > gcBefore)
                        blockers.put(potentialBlocker, sstable);
                }
            }
        }
        return blockers;
    }

    @VisibleForTesting
    public static String formatForExpiryTracing(boolean humanReadable, Iterable<SSTableReader> sstables)
    {
        StringBuilder sb = new StringBuilder();

        for (SSTableReader sstable : sstables)
        {
            long minTimestamp = sstable.getMinTimestamp();
            long maxTimestamp = sstable.getMaxTimestamp();
            long maxLocalDeletionTime = sstable.getMaxLocalDeletionTime();
            if (humanReadable)
            {
                sb.append(logEntry(sstable,
                                   minTimestamp != Long.MIN_VALUE && minTimestamp != Long.MAX_VALUE ? Instant.ofEpochMilli(minTimestamp) : minTimestamp,
                                   maxTimestamp != Long.MIN_VALUE && minTimestamp != Long.MAX_VALUE ? Instant.ofEpochMilli(maxTimestamp) : maxTimestamp,
                                   maxLocalDeletionTime != Long.MAX_VALUE ? Instant.ofEpochSecond(maxLocalDeletionTime) : maxLocalDeletionTime,
                                   FileUtils.stringifyFileSize(sstable.onDiskLength())));
            }
            else
            {
                sb.append(logEntry(sstable,
                                   minTimestamp,
                                   maxTimestamp,
                                   maxLocalDeletionTime,
                                   sstable.onDiskLength()));
            }
            sb.append(", ");
        }

        return sb.toString();
    }

    private static String logEntry(SSTableReader sstable, Object minTs, Object maxTs, Object maxLocationDeletionTime, Object size)
    {
        return String.format("[%s (minTS = %s, maxTS = %s, maxLDT = %s, diskSize = %s)]",
                             sstable, minTs, maxTs, maxLocationDeletionTime, size);
    }
}
