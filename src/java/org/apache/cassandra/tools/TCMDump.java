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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogReader;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static com.google.common.base.Throwables.getStackTraceAsString;

/**
 * Standalone tool to dump Transactional Cluster Metadata (TCM) from local SSTables.
 * <p>
 * This is an emergency recovery tool for debugging when a Cassandra instance cannot
 * start due to TCM issues. It reads the local_metadata_log and metadata_snapshots
 * tables from the system keyspace to reconstruct and display the cluster metadata state.
 * <p>
 * Usage:
 * <pre>
 * # Binary dump (default)
 * tcmdump dump --data-dir /path/to/data
 *
 * # toString output for debugging
 * tcmdump dump --data-dir /path/to/data --to-string
 *
 * # Dump log entries
 * tcmdump dump --data-dir /path/to/data --dump-log --from-epoch 1 --to-epoch 50
 *
 * # Dump distributed log (CMS nodes)
 * tcmdump dump --data-dir /path/to/data --dump-distributed-log
 * </pre>
 */
@Command(name = "tcmdump",
mixinStandardHelpOptions = true,
description = "Dump Transactional Cluster Metadata from local SSTables",
subcommands = { TCMDump.DumpMetadata.class })
public class TCMDump implements Runnable
{
    private static final Output output = Output.CONSOLE;

    public static void main(String... args)
    {
        Util.initDatabaseDescriptor();

        CommandLine cli = new CommandLine(TCMDump.class).setExecutionExceptionHandler((ex, cmd, parseResult) -> {
            err(ex);
            return 2;
        });
        int status = cli.execute(args);
        System.exit(status);
    }

    protected static void err(Throwable e)
    {
        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
    }

    @Override
    public void run()
    {
        CommandLine.usage(this, output.out);
    }

    @Command(name = "dump", description = "Dump cluster metadata from SSTables")
    public static class DumpMetadata implements Runnable
    {
        @Option(names = { "-d", "--data-dir" }, description = "Data directory containing system keyspace")
        public String dataDir;

        @Option(names = { "-s", "--sstables" }, description = "Path to SSTable directory for metadata tables (can be specified multiple times for log and snapshot tables)", arity = "1..*")
        public List<String> sstables;

        @Option(names = { "-p", "--partitioner" }, description = "Partitioner class name",
        defaultValue = "org.apache.cassandra.dht.Murmur3Partitioner")
        public String partitioner;

        @Option(names = { "-o", "--output" }, description = "Output file path for binary dump (default: temp file)")
        public String outputFile;

        // Output modes
        @Option(names = { "--to-string" }, description = "Print ClusterMetadata.toString() to stdout")
        public boolean toStringOutput;

        @Option(names = { "--dump-log" }, description = "Dump log entries (toString each entry)")
        public boolean dumpLog;

        @Option(names = { "--dump-distributed-log" }, description = "Dump distributed_metadata_log (for CMS nodes)")
        public boolean dumpDistributedLog;

        // Filters
        @Option(names = { "--epoch" }, description = "Show state at specific epoch")
        public Long targetEpoch;

        @Option(names = { "--from-epoch" }, description = "Filter log entries from this epoch")
        public Long fromEpoch;

        @Option(names = { "--to-epoch" }, description = "Filter log entries to this epoch")
        public Long toEpoch;

        // Debug
        @Option(names = { "-v", "--verbose" }, description = "Verbose output")
        public boolean verbose;

        @Option(names = { "--debug" }, description = "Show stack traces on errors")
        public boolean debug;

        private Path tempDir;

        /**
         * Gets the log state from the given storage, detecting and logging gaps in epochs.
         * <p>
         * It detects gaps in the epoch sequence and logs warnings instead of throwing exceptions, allowing the tool to
         * still output all available epochs.
         *
         * @param storage         the storage to read entries from
         * @param snapshotManager the snapshot manager for base state
         * @param targetEpoch     optional target epoch to filter to (null for all epochs)
         * @param out             the output to write warnings to
         * @return the LogState with all available entries
         */
        @VisibleForTesting
        static LogState getLogState(SystemKeyspaceStorage storage,
                                    MetadataSnapshots snapshotManager,
                                    Long targetEpoch,
                                    Output out)
        {
            ClusterMetadata base = snapshotManager.getLatestSnapshot();
            Epoch baseEpoch = base == null ? Epoch.EMPTY : base.epoch;
            Epoch endEpoch = targetEpoch != null ? Epoch.create(targetEpoch) : Epoch.create(Long.MAX_VALUE);

            try
            {
                LogReader.EntryHolder entryHolder = storage.getEntries(baseEpoch, endEpoch);
                ImmutableList.Builder<Entry> entries = ImmutableList.builder();
                Epoch prevEpoch = baseEpoch;
                List<String> gaps = new ArrayList<>();

                for (Entry e : (Iterable<Entry>) entryHolder::iterator)
                {
                    if (!prevEpoch.nextEpoch().is(e.epoch))
                    {
                        gaps.add(String.format("Gap detected: expected epoch %d but found %d",
                                               prevEpoch.getEpoch() + 1, e.epoch.getEpoch()));
                    }
                    prevEpoch = e.epoch;
                    entries.add(e);
                }

                if (!gaps.isEmpty())
                {
                    out.err.println("WARNING: Found " + gaps.size() + " gap(s) in the epoch sequence:");
                    for (String gap : gaps)
                    {
                        out.err.println("  " + gap);
                    }
                    out.err.println("Proceeding with available epochs...");
                }

                ImmutableList<Entry> entryList = entries.build();
                // If there's a gap between the base state and the first entry, we need to pass null
                // as the base state to avoid the LogState constructor invariant check failing
                ClusterMetadata effectiveBase = base;
                if (effectiveBase != null && !entryList.isEmpty() && !entryList.get(0).epoch.isDirectlyAfter(effectiveBase.epoch))
                {
                    out.err.println("WARNING: Gap between snapshot (epoch " + effectiveBase.epoch.getEpoch() +
                                    ") and first log entry (epoch " + entryList.get(0).epoch.getEpoch() +
                                    "). Proceeding without base snapshot.");
                    effectiveBase = null;
                }

                return new LogState(effectiveBase, entryList);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed to read log entries", e);
            }
        }

        @Override
        public void run()
        {
            try
            {
                // Create temporary directory for SSTable import
                setupTempDirectory();

                DatabaseDescriptor.setPartitioner(partitioner);

                if (dumpDistributedLog)
                {
                    // Set up schema for distributed metadata keyspace
                    // Use a dummy datacenter name since we're just reading SSTables offline
                    ClusterMetadataService.empty(Keyspaces.of(SystemKeyspace.metadata(), DistributedMetadataLogKeyspace.initialMetadata("dc1")));
                }
                else
                {
                    // Set up minimal schema for system keyspace only
                    ClusterMetadataService.empty(Keyspaces.of(SystemKeyspace.metadata()));
                }
                Keyspace.setInitialized();

                if (dumpLog)
                {
                    importSSTables();
                    LogState logState = getLogState();
                    dumpLogEntries(logState);
                }
                else if (dumpDistributedLog)
                {
                    importDistributedLogSSTables();
                    dumpDistributedLogEntries();
                }
                else
                {
                    importSSTables();
                    LogState logState = getLogState();

                    if (logState.isEmpty())
                    {
                        output.out.println("No metadata available");
                        return;
                    }

                    ClusterMetadata metadata = logState.flatten().baseState;

                    if (toStringOutput)
                    {
                        output.out.println(metadata.toString());
                    }
                    else
                    {
                        dumpBinary(metadata);
                    }
                }
            }
            catch (Exception e)
            {
                if (debug)
                {
                    e.printStackTrace(output.err);
                }
                else
                {
                    output.err.println("Error: " + e.getMessage());
                }
                System.exit(1);
            }
            finally
            {
                cleanupTempDirectory();
            }
        }

        /**
         * Creates a temporary directory and configures DatabaseDescriptor to use it.
         * This ensures we don't pollute any existing data directories.
         */
        private void setupTempDirectory() throws IOException
        {
            tempDir = Files.createTempDirectory("tcmdump");
            DatabaseDescriptor.getRawConfig().data_file_directories = new String[]{ tempDir.resolve("data").toString() };
            DatabaseDescriptor.getRawConfig().commitlog_directory = tempDir.resolve("commitlog").toString();
            DatabaseDescriptor.getRawConfig().hints_directory = tempDir.resolve("hints").toString();
            DatabaseDescriptor.getRawConfig().saved_caches_directory = tempDir.resolve("saved_caches").toString();

            if (verbose)
            {
                output.out.println("Using temporary directory: " + tempDir);
            }
        }

        /**
         * Cleans up the temporary directory.
         */
        private void cleanupTempDirectory()
        {
            if (tempDir != null)
            {
                try
                {
                    Files.walkFileTree(tempDir, new SimpleFileVisitor<>()
                    {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException
                        {
                            Files.deleteIfExists(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                        throws IOException
                        {
                            Files.deleteIfExists(dir);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
                catch (IOException e)
                {
                    if (verbose)
                    {
                        output.err.println("Warning: Failed to fully cleanup temp directory: " + tempDir + " (" + e.getMessage() + ")");
                    }
                }
                finally
                {
                    // Avoid accidental reuse
                    tempDir = null;
                }
            }
        }

        /**
         * Dumps ClusterMetadata to a binary file using VerboseMetadataSerializer.
         * This is the same format used by ClusterMetadataService.dumpClusterMetadata().
         */
        private void dumpBinary(ClusterMetadata metadata) throws IOException
        {
            Path outputPath = outputFile != null ? Path.of(outputFile) : Files.createTempFile("clustermetadata", ".dump");
            try (FileOutputStreamPlus out = new FileOutputStreamPlus(outputPath))
            {
                VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, out, NodeVersion.CURRENT.serializationVersion());
            }
            output.out.println("Dumped cluster metadata to " + outputPath);
        }

        /**
         * Gets the log state from system keyspace, optionally filtered to a target epoch.
         */
        private LogState getLogState()
        {
            MetadataSnapshots snapshotManager = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshotManager);
            return getLogState(storage, snapshotManager, targetEpoch, output);
        }

        /**
         * Dumps log entries to stdout, each as toString().
         */
        private void dumpLogEntries(LogState logState)
        {
            Epoch from = fromEpoch != null ? Epoch.create(fromEpoch) : Epoch.EMPTY;
            Epoch to = toEpoch != null ? Epoch.create(toEpoch) : Epoch.create(Long.MAX_VALUE);

            for (Entry entry : logState.entries)
            {
                if (!entry.epoch.isBefore(from) && !entry.epoch.isAfter(to))
                {
                    output.out.println(entry.toString());
                }
            }
        }

        /**
         * Dumps distributed log entries from system_cluster_metadata.distributed_metadata_log.
         */
        private void dumpDistributedLogEntries()
        {
            Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.METADATA_KEYSPACE_NAME);
            ColumnFamilyStore cfs = ks.getColumnFamilyStore(DistributedMetadataLogKeyspace.TABLE_NAME);

            Epoch from = fromEpoch != null ? Epoch.create(fromEpoch) : Epoch.EMPTY;
            Epoch to = toEpoch != null ? Epoch.create(toEpoch) : Epoch.create(Long.MAX_VALUE);

            // Read log entries from the CFS
            MetadataSnapshots snapshotManager = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshotManager);
            LogState logState = storage.getPersistedLogState();

            for (Entry entry : logState.entries)
            {
                if (!entry.epoch.isBefore(from) && !entry.epoch.isAfter(to))
                {
                    output.out.println(entry.toString());
                }
            }
        }

        private void importSSTables() throws IOException
        {
            Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.SYSTEM_KEYSPACE_NAME);

            // Find and import SSTables for local_metadata_log
            String logTablePath = findTablePath(SystemKeyspace.METADATA_LOG, SchemaConstants.SYSTEM_KEYSPACE_NAME);
            if (logTablePath != null)
            {
                ColumnFamilyStore logCfs = ks.getColumnFamilyStore(SystemKeyspace.METADATA_LOG);
                logCfs.importNewSSTables(Collections.singleton(logTablePath), false, false, false, false, false, false, true);
                if (verbose)
                {
                    output.out.println("Imported SSTables from: " + logTablePath);
                }
            }

            // Find and import SSTables for metadata_snapshots
            String snapshotTablePath = findTablePath(SystemKeyspace.SNAPSHOT_TABLE_NAME, SchemaConstants.SYSTEM_KEYSPACE_NAME);
            if (snapshotTablePath != null)
            {
                ColumnFamilyStore snapshotCfs = ks.getColumnFamilyStore(SystemKeyspace.SNAPSHOT_TABLE_NAME);
                snapshotCfs.importNewSSTables(Collections.singleton(snapshotTablePath), false, false, false, false, false, false, true);
                if (verbose)
                {
                    output.out.println("Imported SSTables from: " + snapshotTablePath);
                }
            }
        }

        private void importDistributedLogSSTables() throws IOException
        {
            Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.METADATA_KEYSPACE_NAME);

            // Find and import SSTables for distributed_metadata_log
            String logTablePath = findTablePath(DistributedMetadataLogKeyspace.TABLE_NAME, SchemaConstants.METADATA_KEYSPACE_NAME);
            if (logTablePath != null)
            {
                ColumnFamilyStore logCfs = ks.getColumnFamilyStore(DistributedMetadataLogKeyspace.TABLE_NAME);
                logCfs.importNewSSTables(Collections.singleton(logTablePath), false, false, false, false, false, false, true);
                if (verbose)
                {
                    output.out.println("Imported SSTables from: " + logTablePath);
                }
            }
        }

        private String findTablePath(String tableName, String keyspaceName) throws IOException
        {
            if (sstables != null && !sstables.isEmpty())
            {
                // User provided explicit paths - only search within those
                for (String sstablePath : sstables)
                {
                    // Check if this path is for the specific table
                    if (sstablePath.contains(tableName))
                        return sstablePath;
                    // Check if it's a parent directory containing the table
                    Path tableDir = Path.of(sstablePath, tableName);
                    if (Files.exists(tableDir))
                        return tableDir.toString();
                    // Check if it's a keyspace directory containing the table
                    String matches = findTablePathInDir(tableName, keyspaceName, sstablePath);
                    if (matches != null)
                        return matches;
                }
                // No fallback if --sstables are supplied
                return null;
            }

            if (dataDir != null)
            {
                // Discover from data directory
                String matches = findTablePathInDir(tableName, keyspaceName, dataDir);
                if (matches != null)
                    return matches;
            }

            // Try default data directories from cassandra.yaml
            String[] dataDirs = DatabaseDescriptor.getAllDataFileLocations();
            for (String dir : dataDirs)
            {
                String matches = findTablePathInDir(tableName, keyspaceName, dir);
                if (matches != null)
                    return matches;
            }

            return null;
        }

        private String findTablePathInDir(String tableName, String keyspaceName, String dataDir) throws IOException
        {
            Path ksDir = Path.of(dataDir, keyspaceName);
            if (Files.exists(ksDir))
            {
                try (Stream<Path> paths = Files.list(ksDir))
                {
                    List<Path> matches = paths.filter(p -> p.getFileName().toString().startsWith(tableName + "-"))
                                              .collect(Collectors.toList());
                    if (!matches.isEmpty())
                        return matches.get(0).toString();
                }
            }
            return null;
        }
    }
}
