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
package org.apache.cassandra.service.snapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;
import oshi.PlatformEnum;

public class TableSnapshot
{
    private static final Logger logger = LoggerFactory.getLogger(TableSnapshot.class);
    private static final PlatformEnum PLATFORM = FBUtilities.getSystemInfo().platform();

    private final String keyspaceName;
    private final String tableName;
    private final String keyspaceTable;
    private final UUID tableId;
    private final String tag;
    private final boolean ephemeral;

    private final Instant createdAt;
    private final Instant expiresAt;

    private final Set<File> snapshotDirs;

    private volatile long sizeOnDisk = 0;

    private volatile long manifestsSize;
    private volatile long schemasSize;

    private volatile boolean inProgress = false;

    public TableSnapshot(String keyspaceName, String tableName, UUID tableId,
                         String tag, Instant createdAt, Instant expiresAt,
                         Set<File> snapshotDirs, boolean ephemeral)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.keyspaceTable = keyspaceName + '.' + tableName;
        this.tableId = tableId;
        this.tag = tag;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.snapshotDirs = snapshotDirs;
        this.ephemeral = ephemeral;

        manifestsSize = getManifestsSize();
        schemasSize = getSchemasSize();
    }

    public boolean isCompleted()
    {
        return !inProgress;
    }

    public void incomplete()
    {
        inProgress = true;
    }

    public void complete()
    {
        inProgress = false;
    }

    /**
     * Unique identifier of a snapshot. Used
     * only to deduplicate snapshots internally,
     * not exposed externally.
     * <p>
     * Format: "$ks:$table_name:$table_id:$tag"
     */
    public String getId()
    {
        return buildSnapshotId(keyspaceName, tableName, tableId, tag);
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getKeyspaceTable()
    {
        return keyspaceTable;
    }

    public String getTag()
    {
        return tag;
    }

    public Instant getCreatedAt()
    {
        if (createdAt == null)
        {
            long minCreation = 0;
            for (File snapshotDir : snapshotDirs)
            {
                if (!snapshotDir.exists())
                    continue;

                long lastModified = snapshotDir.lastModified();
                if (lastModified == 0)
                    continue;

                if (minCreation == 0 || minCreation > lastModified)
                    minCreation = lastModified;
            }

            if (minCreation != 0)
                return Instant.ofEpochMilli(minCreation);
        }
        return createdAt;
    }

    public Instant getExpiresAt()
    {
        return expiresAt;
    }

    public boolean isExpired(Instant now)
    {
        if (createdAt == null || expiresAt == null)
        {
            return false;
        }

        return expiresAt.compareTo(now) < 0;
    }

    public boolean exists()
    {
        for (File snapshotDir : snapshotDirs)
            if (snapshotDir.exists())
                return true;

        return false;
    }

    public boolean isEphemeral()
    {
        return ephemeral;
    }

    public boolean isExpiring()
    {
        return expiresAt != null;
    }

    public long computeSizeOnDiskBytes()
    {
        long sum = sizeOnDisk;
        if (sum == 0)
        {
            for (File snapshotDir : snapshotDirs)
                sum += FileUtils.folderSize(snapshotDir);

            sizeOnDisk = sum;
        }

        return sum;
    }

    public long computeTrueSizeBytes()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspaceName, tableName);
        if (cfs == null)
            return 0;

        return computeTrueSizeBytes(cfs.getFilesOfCfs());
    }

    public long computeTrueSizeBytes(Set<String> files)
    {
        long size = manifestsSize + schemasSize;

        for (File dataPath : getDirectories())
        {
            List<Path> snapshotFiles = listDir(dataPath.toPath());
            for (Path snapshotFile : snapshotFiles)
            {
                if (!snapshotFile.endsWith("manifest.json") && !snapshotFile.endsWith("schema.cql"))
                {
                    // files == null means that the underlying table was most probably dropped
                    // so in that case we indeed go to count snapshots file in for true size
                    if (files == null || (!files.contains(getLiveFileFromSnapshotFile(snapshotFile))))
                        size += getFileSize(snapshotFile);
                }
            }
        }

        return size;
    }

    private List<Path> listDir(Path dir)
    {
        List<Path> paths = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir))
        {
            stream.forEach(p -> {
                if (p.getFileName().toString().startsWith("."))
                {
                    paths.addAll(listDir(p));
                }
                else
                {
                    paths.add(p);
                }
            });
        }
        catch (Throwable t)
        {
            logger.error("Could not list directory content {}", dir, t);
        }

        return paths;
    }

    private long getFileSize(Path file)
    {
        try
        {
            return Files.size(file);
        }
        catch (Throwable t)
        {
            return 0;
        }
    }

    /**
     * Returns the corresponding live file for a given snapshot file.
     * <p>
     * Example:
     * - Base table:
     * - Snapshot file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/snapshots/1643481737850/me-1-big-Data.db
     * - Live file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/me-1-big-Data.db
     * - Secondary index:
     * - Snapshot file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/snapshots/1643481737850/.tbl_val_idx/me-1-big-Summary.db
     * - Live file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/.tbl_val_idx/me-1-big-Summary.db
     */
    static String getLiveFileFromSnapshotFile(Path snapshotFilePath)
    {
        // Snapshot directory structure format is {data_dir}/snapshots/{snapshot_name}/{snapshot_file}
        Path liveDir = snapshotFilePath.getParent().getParent().getParent();
        if (Directories.isSecondaryIndexFolder(snapshotFilePath.getParent()))
        {
            // Snapshot file structure format is {data_dir}/snapshots/{snapshot_name}/.{index}/{sstable-component}.db
            liveDir = File.getPath(liveDir.getParent().toString(), snapshotFilePath.getParent().getFileName().toString());
        }
        return liveDir.resolve(snapshotFilePath.getFileName().toString()).toAbsolutePath().toString();
    }

    public Collection<File> getDirectories()
    {
        return snapshotDirs;
    }

    /**
     * Returns all manifest files of a snapshot.
     * <p>
     * In practice, there might be multiple manifest files, as many as we have snapshot dirs.
     * Each snapshot dir will hold its view of a snapshot, containing only sstables located in such snapshot dir.
     *
     * @return all manifest files
     */
    public Set<File> getManifestFiles()
    {
        Set<File> manifestFiles = new HashSet<>();
        for (File snapshotDir : snapshotDirs)
        {
            File manifestFile = Directories.getSnapshotManifestFile(snapshotDir);
            if (manifestFile.exists())
                manifestFiles.add(manifestFile);
        }
        return manifestFiles;
    }

    public boolean hasManifest()
    {
        for (File snapshotDir : snapshotDirs)
        {
            if (Directories.getSnapshotManifestFile(snapshotDir).exists())
                return true;
        }
        return false;
    }

    /**
     * Returns all schemas files of a snapshot.
     *
     * @return all schema files
     */
    public Set<File> getSchemaFiles()
    {
        Set<File> schemaFiles = new HashSet<>();
        for (File snapshotDir : snapshotDirs)
        {
            File schemaFile = Directories.getSnapshotSchemaFile(snapshotDir);
            if (schemaFile.exists())
                schemaFiles.add(schemaFile);
        }
        return schemaFiles;
    }

    public long getManifestsSize()
    {
        long size = 0;
        for (File manifestFile : getManifestFiles())
            size += manifestFile.length();

        return size;
    }

    public long getSchemasSize()
    {
        long size = 0;
        for (File schemaFile : getSchemaFiles())
            size += schemaFile.length();

        return size;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSnapshot snapshot = (TableSnapshot) o;
        return Objects.equals(keyspaceName, snapshot.keyspaceName) &&
               Objects.equals(tableName, snapshot.tableName) &&
               Objects.equals(tableId, snapshot.tableId) &&
               tagsEqual(tag, snapshot.tag);
    }

    private boolean tagsEqual(String tag1, String tag2)
    {
        if (tag1 == null && tag2 == null)
            return true;

        if (tag1 == null || tag2 == null)
            return false;

        // When hardlinks are created for a snapshot with the name "snapshot"
        // and then we take a snapshot with the name "Snapshot", macOS platform thinks
        // that this was already hardlinked because its hardlinking implementation
        // does not seem to be case-sensitive. The fix consists of checking,
        // in a case-insensitive manner, if there is already such snapshot,
        // but only on macOS platform.
        if (PLATFORM == PlatformEnum.MACOS)
            return LocalizeString.toLowerCaseLocalized(tag1).equals(LocalizeString.toLowerCaseLocalized(tag2));

        return Objects.equals(tag1, tag2);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaceName, tableName, tableId, tag);
    }

    @Override
    public String toString()
    {
        return "TableSnapshot{" +
               "keyspaceName='" + keyspaceName + '\'' +
               ", tableName='" + tableName + '\'' +
               ", tableId=" + tableId +
               ", tag='" + tag + '\'' +
               ", createdAt=" + createdAt +
               ", expiresAt=" + expiresAt +
               ", snapshotDirs=" + snapshotDirs +
               ", ephemeral=" + ephemeral +
               '}';
    }

    public void updateMetadataSize()
    {
        if (manifestsSize == 0)
            manifestsSize = getManifestsSize();
        if (schemasSize == 0)
            schemasSize = getSchemasSize();
    }

    static class Builder
    {
        private final String keyspaceName;
        private final String tableName;
        private final UUID tableId;
        private final String tag;

        private Instant createdAt = null;
        private Instant expiresAt = null;
        private boolean ephemeral;

        private final Set<File> snapshotDirs = new HashSet<>();

        Builder(String keyspaceName, String tableName, UUID tableId, String tag)
        {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.tag = tag;
            this.tableId = tableId;
        }

        void addSnapshotDir(File snapshotDir)
        {
            snapshotDirs.add(snapshotDir);
            File manifestFile = new File(snapshotDir, "manifest.json");
            if (manifestFile.exists() && createdAt == null && expiresAt == null)
                loadMetadataFromManifest(manifestFile);

            // check if an ephemeral marker file exists only in case it is not already ephemeral
            // by reading it from manifest
            // TODO remove this on Cassandra 4.3 release, see CASSANDRA-16911
            if (!ephemeral && new File(snapshotDir, "ephemeral.snapshot").exists())
                ephemeral = true;
        }

        private void loadMetadataFromManifest(File manifestFile)
        {
            try
            {
                logger.trace("Loading snapshot manifest from {}", manifestFile);
                SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);
                createdAt = manifest.createdAt;
                expiresAt = manifest.expiresAt;
                // a snapshot may be ephemeral when it has a marker file (old way) or flag in manifest (new way)
                if (!ephemeral)
                    ephemeral = manifest.ephemeral;
            }
            catch (IOException e)
            {
                logger.warn("Cannot read manifest file {} of snapshot {}.", manifestFile, tag, e);
            }
        }

        TableSnapshot build()
        {
            return new TableSnapshot(keyspaceName, tableName, tableId, tag, createdAt, expiresAt, snapshotDirs, ephemeral);
        }
    }

    protected static String buildSnapshotId(String keyspaceName, String tableName, UUID tableId, String tag)
    {
        return String.format("%s:%s:%s:%s", keyspaceName, tableName, tableId, tag);
    }

    public static Set<Descriptor> getSnapshotDescriptors(String keyspace, String table, String tag)
    {
        try
        {
            Refs<SSTableReader> snapshotSSTableReaders = getSnapshotSSTableReaders(keyspace, table, tag);

            Set<Descriptor> descriptors = new HashSet<>();
            for (SSTableReader ssTableReader : snapshotSSTableReaders)
            {
                descriptors.add(ssTableReader.descriptor);
            }

            return descriptors;
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    public static Refs<SSTableReader> getSnapshotSSTableReaders(String keyspace, String table, String tag) throws IOException
    {
        return getSnapshotSSTableReaders(Keyspace.open(keyspace).getColumnFamilyStore(table), tag);
    }

    public static Refs<SSTableReader> getSnapshotSSTableReaders(ColumnFamilyStore cfs, String tag)
    {
        Map<SSTableId, SSTableReader> active = new HashMap<>();
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            active.put(sstable.descriptor.id, sstable);
        Map<Descriptor, Set<Component>> snapshots = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(tag).list();
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            for (Map.Entry<Descriptor, Set<Component>> entries : snapshots.entrySet())
            {
                // Try to acquire reference to an active sstable instead of snapshot if it exists,
                // to avoid opening new sstables. If it fails, use the snapshot reference instead.
                SSTableReader sstable = active.get(entries.getKey().id);
                if (sstable == null || !refs.tryRef(sstable))
                {
                    if (logger.isTraceEnabled())
                        logger.trace("using snapshot sstable {}", entries.getKey());
                    // open offline so we don't modify components or track hotness.
                    sstable = SSTableReader.open(cfs, entries.getKey(), entries.getValue(), cfs.metadata, true, true);
                    refs.tryRef(sstable);
                    // release the self ref as we never add the snapshot sstable to DataTracker where it is otherwise released
                    sstable.selfRef().release();
                }
                else if (logger.isTraceEnabled())
                {
                    logger.trace("using active sstable {}", entries.getKey());
                }
            }
        }
        catch (FSReadError | RuntimeException e)
        {
            // In case one of the snapshot sstables fails to open,
            // we must release the references to the ones we opened so far
            refs.release();
            throw e;
        }
        return refs;
    }

    public static String getTimestampedSnapshotName(String clientSuppliedName, long timestamp)
    {
        String snapshotName = Long.toString(timestamp);
        if (clientSuppliedName != null && !clientSuppliedName.isEmpty())
            snapshotName = snapshotName + '-' + clientSuppliedName;

        return snapshotName;
    }

    public static String getTimestampedSnapshotNameWithPrefix(String clientSuppliedName, long timestamp, String prefix)
    {
        return prefix + '-' + getTimestampedSnapshotName(clientSuppliedName, timestamp);
    }
}
