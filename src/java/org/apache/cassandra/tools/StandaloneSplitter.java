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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class StandaloneSplitter
{
    public static final int DEFAULT_SSTABLE_SIZE = 50;

    @VisibleForTesting
    private static volatile boolean failSnapshotForTesting;

    private static final String TOOL_NAME = "sstablessplit";
    private static final String DEBUG_OPTION = "debug";
    private static final String HELP_OPTION = "help";
    private static final String NO_SNAPSHOT_OPTION = "no-snapshot";
    private static final String SIZE_OPTION = "size";
    private static final String ZERO_COPY_OPTION = "zero-copy";

    public static void main(String[] args)
    {
        Options options = Options.parseArgs(args);
        if (TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.getBoolean())
            DatabaseDescriptor.toolInitialization(false); //Necessary for testing
        else
            Util.initDatabaseDescriptor();

        try
        {
            ClusterMetadataService.initializeForTools(false);
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

                Descriptor desc = SSTable.tryDescriptorFromFile(file);
                if (desc == null) {
                    System.out.println("Skipping non sstable file " + file);
                    continue;
                }

                if (ksName == null)
                    ksName = desc.ksname;
                else if (!ksName.equals(desc.ksname))
                    throw new IllegalArgumentException("All sstables must be part of the same keyspace");

                if (cfName == null)
                    cfName = desc.cfname;
                else if (!cfName.equals(desc.cfname))
                    throw new IllegalArgumentException("All sstables must be part of the same table");

                Set<Component> components;
                if (options.zeroCopy)
                    components = TOCComponent.loadOrCreate(desc);
                else
                    components = desc.getComponents(Collections.emptySet(), desc.getFormat().batchComponents());
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
            if (options.zeroCopy && cfs.indexManager.hasSSTableAttachedIndexes())
                throw new UnsupportedOperationException("Cannot zero-copy split SSTables for " + ksName + '.' + cfName +
                                                        " because the table has SSTable-attached indexes");

            String snapshotName = "pre-split-" + currentTimeMillis();

            List<SSTableReader> sstables = new ArrayList<>();
            int snapshotted = 0;
            boolean failed = false;
            for (Map.Entry<Descriptor, Set<Component>> fn : parsedFilenames.entrySet())
            {
                SSTableReader sstable = null;
                try
                {
                    sstable = SSTableReader.openNoValidation(fn.getKey(), fn.getValue(), cfs);
                    if (!isSSTableLargerEnough(sstable, options.sizeInMB)) {
                        System.out.printf("Skipping %s: it's size (%.3f MB) is less than the split size (%d MB)%n",
                                          sstable.getFilename(), ((sstable.onDiskLength() * 1.0d) / 1024L) / 1024L, options.sizeInMB);
                        sstable.selfRef().ensureReleased();
                        sstable = null;
                        continue;
                    }

                    if (options.snapshot) {
                        File snapshotDirectory = null;
                        List<File> snapshotLinks = new ArrayList<>();
                        try
                        {
                            snapshotDirectory = Directories.getSnapshotDirectory(sstable.descriptor, snapshotName);
                            createSnapshotLinks(sstable, snapshotDirectory, snapshotLinks);
                            if (failSnapshotForTesting)
                                throw new RuntimeException("Snapshot failure injected for testing");
                            snapshotted++;
                        }
                        catch (Exception | FSError e)
                        {
                            if (snapshotDirectory != null)
                                removeIncompleteSnapshotLinks(snapshotLinks, snapshotDirectory);
                            failed = true;
                            JVMStabilityInspector.inspectThrowable(e);
                            System.err.printf("Error Snapshotting %s: %s%n", fn.getKey(), e.getMessage());
                            if (options.debug)
                                e.printStackTrace(System.err);
                            sstable.selfRef().ensureReleased();
                            sstable = null;
                            continue;
                        }
                    }

                    // Do not split an sstable without a complete snapshot. In particular, createLinks may have
                    // created some links before a later component failed, but those links are not a usable rollback.
                    sstables.add(sstable);
                }
                catch (Exception | FSError e)
                {
                    failed = true;
                    JVMStabilityInspector.inspectThrowable(e);
                    System.err.printf("Error Loading %s: %s%n", fn.getKey(), e.getMessage());
                    if (options.debug)
                        e.printStackTrace(System.err);
                    if (sstable != null)
                        sstable.selfRef().ensureReleased();
                }
            }
            if (sstables.isEmpty()) {
                System.out.println("No sstables needed splitting.");
                System.exit(failed ? 1 : 0);
            }
            if (options.snapshot)
                System.out.printf("Pre-split %d sstable(s) snapshotted into snapshot %s%n", snapshotted, snapshotName);

            for (SSTableReader sstable : sstables)
            {
                try (LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.UNKNOWN, sstable))
                {
                    if (options.zeroCopy)
                    {
                        ZeroCopySSTableSplitter.Result result = zeroCopySplit(sstable, transaction, options.sizeInMB);
                        long bytesWritten = result.totalBytesWritten();
                        System.out.printf("Zero-copy split committed: children=%d, bytes cloned=%d, bytes written=%d, " +
                                          "reflink used=%s%n",
                                          result.children.size(), result.totalBytesCloned, bytesWritten,
                                          result.totalBytesCloned > 0 ? "yes" : "no");
                    }
                    else
                        new SSTableSplitter(cfs, transaction, options.sizeInMB).split();
                }
                catch (Exception e)
                {
                    failed = true;
                    System.err.printf("Error splitting %s: %s%n", sstable, e.getMessage());
                    if (options.debug)
                        e.printStackTrace(System.err);
                }
                finally
                {
                    // Commit or abort normally releases the original. Also cover a transaction-construction failure
                    // without double-releasing an original the transaction already consumed.
                    sstable.selfRef().ensureReleased();
                }
            }
            CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
            LifecycleTransaction.waitForDeletions();
            System.exit(failed ? 1 : 0); // We need that to stop non daemonized threads
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

    private static ZeroCopySSTableSplitter.Result zeroCopySplit(SSTableReader sstable,
                                                                LifecycleTransaction transaction,
                                                                int sizeInMB)
    {
        long targetSize = sizeInMB * 1024L * 1024L;
        ZeroCopySSTableSplitter.Result result = ZeroCopySSTableSplitter.splitBySize(sstable,
                                                                                    targetSize,
                                                                                    transaction);

        int updated = 0;
        try
        {
            for (ZeroCopySSTableSplitter.Child child : result.children)
            {
                transaction.update(child.reader, false);
                updated++;
            }
            transaction.obsoleteOriginals();
            transaction.prepareToCommit();
            transaction.commit();
            return result;
        }
        catch (RuntimeException | Error t)
        {
            // Updated readers belong to the transaction and are released by its abort; the remainder are still ours.
            for (int i = updated; i < result.children.size(); i++)
                result.children.get(i).reader.selfRef().release();
            throw t;
        }
    }

    private static void createSnapshotLinks(SSTableReader sstable, File snapshotDirectory, List<File> createdLinks)
    {
        for (Component component : sstable.getComponents())
        {
            File source = sstable.descriptor.fileFor(component);
            if (!source.exists())
                continue;

            SSTableReader.createLinks(sstable.descriptor,
                                      Collections.singleton(component),
                                      snapshotDirectory.path());
            createdLinks.add(new File(snapshotDirectory, source.name()));
        }
    }

    private static void removeIncompleteSnapshotLinks(List<File> createdLinks, File snapshotDirectory)
    {
        for (File link : createdLinks)
        {
            try
            {
                link.deleteIfExists();
            }
            catch (RuntimeException | FSError e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                System.err.printf("Error cleaning incomplete snapshot link %s: %s%n", link, e.getMessage());
            }
        }
        File snapshotParent = snapshotDirectory.parent();
        if (snapshotDirectory.tryList().length == 0 && snapshotDirectory.tryDelete()
            && snapshotParent.tryList().length == 0)
        {
            snapshotParent.tryDelete();
        }
    }

    @VisibleForTesting
    static void setFailSnapshotForTesting(boolean fail)
    {
        failSnapshotForTesting = fail;
    }

    private static class Options
    {
        public final List<String> filenames;

        public boolean debug;
        public boolean snapshot;
        public boolean zeroCopy;
        public int sizeInMB;

        private Options(List<String> filenames)
        {
            this.filenames = filenames;
        }

        public static Options parseArgs(String[] cmdArgs)
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
                opts.zeroCopy = cmd.hasOption(ZERO_COPY_OPTION);
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
            options.addOption("s",  SIZE_OPTION, "size", "maximum size in MB for the output sstables (default: " + DEFAULT_SSTABLE_SIZE + ')');
            options.addOption(null, ZERO_COPY_OPTION, "copy compressed chunks instead of rewriting partitions");
            return options;
        }

        public static void printUsage(CmdLineOptions options)
        {
            String usage = String.format("%s [options] <filename> [<filename>]*", TOOL_NAME);
            String header = "--\n" +
                            "Split the provided sstables files in sstables of maximum provided file size (see option --" + SIZE_OPTION + ")." +
                            "\n--\n" +
                            "Options are:";
            new HelpFormatter().printHelp(usage, header, options, "");
        }
    }
}
