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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.log.Entry;
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
 * Useful for debugging when a Cassandra instance cannot start due to TCM issues.
 * This tool reads the local_metadata_log and metadata_snapshots tables from the
 * system keyspace to reconstruct and display the cluster metadata state.
 * <p>
 * Usage:
 * sstablemetadatadump dump --data-dir /path/to/data --all
 * sstablemetadatadump dump --data-dir /path/to/data --to-string
 * sstablemetadatadump dump --sstables /path/to/system/local_metadata_log-xxx/ --epochs
 */
@Command(name = "sstablemetadatadump",
mixinStandardHelpOptions = true,
description = "Dump Transactional Cluster Metadata from local SSTables",
subcommands = { SSTableMetadataDump.DumpMetadata.class })
public class SSTableMetadataDump implements Runnable
{
    private static final Output output = Output.CONSOLE;

    public static void main(String... args)
    {
        // Minimal initialization
        Util.initDatabaseDescriptor();

        CommandLine cli = new CommandLine(SSTableMetadataDump.class).setExecutionExceptionHandler((ex, cmd, parseResult) -> {
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

    /**
     * Container for all collected dump data. Data is collected once and used
     * for text output to avoid duplicate queries.
     * <p>
     * Contents:
     * - metadata: Final reconstructed ClusterMetadata (latest snapshot + applied transformations)
     * - logState: Contains base snapshot + list of transformation entries for --epochs display
     * - snapshotEpochs: List of epoch numbers where snapshots exist for --snapshots display
     */
    private static class DumpData
    {
        final ClusterMetadata metadata;
        final LogState logState;
        final List<Epoch> snapshotEpochs;

        DumpData(ClusterMetadata metadata, LogState logState, List<Epoch> snapshotEpochs)
        {
            this.metadata = metadata;
            this.logState = logState;
            this.snapshotEpochs = snapshotEpochs;
        }
    }

    @Command(name = "dump", description = "Dump cluster metadata from SSTables")
    public static class DumpMetadata implements Runnable
    {
        @Option(names = { "-d", "--data-dir" }, description = "Data directory containing system keyspace")
        public String dataDir;

        @Option(names = { "-s", "--sstables" }, description = "Path to SSTable directory for metadata tables")
        public String sstables;

        @Option(names = { "-p", "--partitioner" }, description = "Partitioner class name",
        defaultValue = "org.apache.cassandra.dht.Murmur3Partitioner")
        public String partitioner;

        @Option(names = { "-o", "--output" }, description = "Output file path for binary dump (default: temp file)")
        public String outputFile;

        // Output mode flags
        @Option(names = { "--to-string" }, description = "Print ClusterMetadata.toString() to stdout")
        public boolean toStringOutput;

        @Option(names = { "--text" }, description = "Print detailed text output to stdout")
        public boolean textOutput;

        // Scope flags (used with --text)
        @Option(names = { "--epochs" }, description = "List all epochs and transformation kinds")
        public boolean epochs;

        @Option(names = { "--schema" }, description = "Dump schema information")
        public boolean schema;

        @Option(names = { "--directory" }, description = "Dump node membership/directory")
        public boolean directory;

        @Option(names = { "--tokens" }, description = "Dump token map")
        public boolean tokens;

        @Option(names = { "--snapshots" }, description = "List available snapshots")
        public boolean snapshots;

        @Option(names = { "--transformations" }, description = "Dump full transformation details")
        public boolean transformations;

        @Option(names = { "--all" }, description = "Include everything")
        public boolean all;

        // Filters
        @Option(names = { "--epoch" }, description = "Show state at specific epoch")
        public Long targetEpoch;

        @Option(names = { "--from-epoch" }, description = "Filter from this epoch")
        public Long fromEpoch;

        @Option(names = { "--to-epoch" }, description = "Filter to this epoch")
        public Long toEpoch;

        // Debug
        @Option(names = { "-v", "--verbose" }, description = "Verbose output")
        public boolean verbose;

        @Option(names = { "--debug" }, description = "Show stack traces on errors")
        public boolean debug;

        @Override
        public void run()
        {
            try
            {
                DatabaseDescriptor.setPartitioner(partitioner);

                // Set up minimal schema for system keyspace
                ClusterMetadataService.empty(Keyspaces.of(SystemKeyspace.metadata()));
                Keyspace.setInitialized();

                importSSTables();

                DumpData data = collectData();

                if (data.metadata == null)
                {
                    output.out.println("No metadata available");
                    return;
                }

                // Handle output modes
                if (toStringOutput)
                {
                    // Print ClusterMetadata.toString() to stdout
                    output.out.println(data.metadata);
                }
                else if (textOutput)
                {
                    // Print detailed text output
                    if (all)
                    {
                        epochs = schema = directory = tokens = snapshots = transformations = true;
                    }
                    outputText(data);
                }
                else
                {
                    // Default: binary dump like ClusterMetadataService.dumpClusterMetadata()
                    dumpBinary(data.metadata);
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
         * Collects all data needed for output. This is done once to avoid
         * creating multiple SystemKeyspaceStorage instances and re-querying.
         * <p>
         * Data flow:
         * 1. getPersistedLogState() returns LogState containing:
         * - baseState: latest snapshot from metadata_snapshots
         * - entries: all transformations after that snapshot
         * 2. flatten() applies transformations to get final ClusterMetadata
         * 3. listSnapshotsSince(EMPTY) returns all snapshot epoch numbers for display
         */
        private DumpData collectData()
        {
            MetadataSnapshots snapshotManager = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshotManager);

            LogState logState = storage.getPersistedLogState();

            ClusterMetadata metadata = null;
            if (!logState.isEmpty())
            {
                if (targetEpoch != null)
                {
                    logState = LogState.getForRecovery(Epoch.create(targetEpoch));
                }

                metadata = logState.flatten().baseState;
            }

            List<Epoch> snapshotEpochs = snapshotManager.listSnapshotsSince(Epoch.EMPTY);

            return new DumpData(metadata, logState, snapshotEpochs);
        }

        private void importSSTables() throws IOException
        {
            Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.SYSTEM_KEYSPACE_NAME);

            // Find and import SSTables for local_metadata_log
            String logTablePath = findTablePath(SystemKeyspace.METADATA_LOG);
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
            String snapshotTablePath = findTablePath(SystemKeyspace.SNAPSHOT_TABLE_NAME);
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

        private String findTablePath(String tableName) throws IOException
        {
            if (sstables != null)
            {
                // User provided explicit path
                if (sstables.contains(tableName))
                    return sstables;
                // Check if it's a parent directory containing the table
                Path tableDir = Path.of(sstables, tableName);
                if (Files.exists(tableDir))
                    return tableDir.toString();
            }

            if (dataDir != null)
            {
                // Discover from data directory
                String matches = findTablePath(tableName, dataDir);
                if (matches != null) return matches;
            }

            // Try default data directories from cassandra.yaml
            String[] dataDirs = DatabaseDescriptor.getAllDataFileLocations();
            for (String dir : dataDirs)
            {
                String matches = findTablePath(tableName, dir);
                if (matches != null) return matches;
            }

            return null;
        }

        private String findTablePath(String tableName, String dataDir) throws IOException
        {
            Path systemDir = Path.of(dataDir, "system");
            if (Files.exists(systemDir))
            {
                try (Stream<Path> paths = Files.list(systemDir))
                {
                    List<Path> matches = paths.filter(p -> p.getFileName().toString().startsWith(tableName + "-"))
                                              .collect(Collectors.toList());
                    if (!matches.isEmpty())
                        return matches.get(0).toString();
                }
            }
            return null;
        }

        /**
         * Filters log entries based on --from-epoch and --to-epoch flags.
         */
        private List<Entry> filterEntries(DumpData data)
        {
            Epoch from = fromEpoch != null ? Epoch.create(fromEpoch) : Epoch.EMPTY;
            Epoch to = toEpoch != null ? Epoch.create(toEpoch) : Epoch.create(Long.MAX_VALUE);

            return data.logState.entries.stream()
                                        .filter(e -> !e.epoch.isBefore(from) && !e.epoch.isAfter(to))
                                        .collect(Collectors.toList());
        }

        private void outputText(DumpData data)
        {
            PrintStream out = output.out;

            out.println("=== Cluster Metadata Dump ===");

            if (data.metadata == null)
            {
                out.println("No metadata available");
                return;
            }

            out.println("Current Epoch: " + data.metadata.epoch);
            out.println("Partitioner: " + data.metadata.partitioner.getClass().getName());
            out.println();

            if (epochs || transformations)
            {
                outputEpochsText(data);
            }

            if (directory)
            {
                outputDirectoryText(data.metadata);
            }

            if (tokens)
            {
                outputTokensText(data.metadata);
            }

            if (schema)
            {
                outputSchemaText(data.metadata);
            }

            if (snapshots)
            {
                outputSnapshotsText(data);
            }
        }

        private void outputEpochsText(DumpData data)
        {
            PrintStream out = output.out;
            out.println("--- Epochs and Transformations ---");

            for (Entry entry : filterEntries(data))
            {
                out.printf("Epoch %d: %s%n", entry.epoch.getEpoch(), entry.transform.kind());

                if (transformations && verbose)
                {
                    out.printf("  Entry ID: %d%n", entry.id.entryId);
                    out.printf("  Transform: %s%n", entry.transform);
                }
            }
            out.println();
        }

        private void outputDirectoryText(ClusterMetadata metadata)
        {
            PrintStream out = output.out;
            out.println("--- Directory (Node Membership) ---");

            metadata.directory.peerIds().forEach(nodeId -> {
                out.printf("NodeId: %s%n", nodeId);
                out.printf("  Address: %s%n", metadata.directory.endpoint(nodeId));
                out.printf("  State: %s%n", metadata.directory.peerState(nodeId));
                out.printf("  DC/Rack: %s/%s%n",
                           metadata.directory.location(nodeId).datacenter,
                           metadata.directory.location(nodeId).rack);
                out.printf("  Host ID: %s%n", metadata.directory.hostId(nodeId));
                out.println();
            });
        }

        private void outputTokensText(ClusterMetadata metadata)
        {
            PrintStream out = output.out;
            out.println("--- Token Map ---");

            metadata.directory.peerIds().forEach(nodeId -> {
                List<Token> nodeTokens = metadata.tokenMap.tokens(nodeId);
                out.printf("NodeId %s: %d tokens%n", nodeId, nodeTokens.size());
                if (verbose)
                {
                    nodeTokens.forEach(token -> out.printf("  %s%n", token));
                }
            });
            out.println();
        }

        private void outputSchemaText(ClusterMetadata metadata)
        {
            PrintStream out = output.out;
            out.println("--- Schema ---");

            metadata.schema.getKeyspaces().forEach(ks -> {
                out.printf("Keyspace: %s%n", ks.name);
                ks.tables.forEach(table -> {
                    out.printf("  Table: %s (id: %s)%n", table.name, table.id);
                });
                out.println();
            });
        }

        private void outputSnapshotsText(DumpData data)
        {
            PrintStream out = output.out;
            out.println("--- Available Snapshots ---");

            if (data.snapshotEpochs.isEmpty())
            {
                out.println("No snapshots found");
            }
            else
            {
                data.snapshotEpochs.forEach(epoch -> out.printf("Snapshot at epoch: %d%n", epoch.getEpoch()));
            }
            out.println();
        }
    }
}
