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
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;

public class TableSnapshot
{
    private static final Logger logger = LoggerFactory.getLogger(TableSnapshot.class);

    private final String keyspaceName;
    private final String tableName;
    private final UUID tableId;
    private final String tag;
    private final boolean ephemeral;

    private final Instant createdAt;
    private final Instant expiresAt;

    private final Set<File> snapshotDirs;

    public TableSnapshot(String keyspaceName, String tableName, UUID tableId,
                         String tag, Instant createdAt, Instant expiresAt,
                         Set<File> snapshotDirs, boolean ephemeral)
    {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.tag = tag;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.snapshotDirs = snapshotDirs;
        this.ephemeral = ephemeral;
    }

    /**
     * Unique identifier of a snapshot. Used
     * only to deduplicate snapshots internally,
     * not exposed externally.
     *
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

    public String getTag()
    {
        return tag;
    }

    public Instant getCreatedAt()
    {
        if (createdAt == null)
        {
            long minCreation = snapshotDirs.stream().mapToLong(File::lastModified).min().orElse(0);
            if (minCreation != 0)
            {
                return Instant.ofEpochMilli(minCreation);
            }
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
        return snapshotDirs.stream().anyMatch(File::exists);
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
        return snapshotDirs.stream().mapToLong(FileUtils::folderSize).sum();
    }

    public long computeTrueSizeBytes()
    {
        DirectorySizeCalculator visitor = new SnapshotTrueSizeCalculator();

        for (File snapshotDir : snapshotDirs)
        {
            try
            {
                Files.walkFileTree(snapshotDir.toPath(), visitor);
            }
            catch (IOException e)
            {
                logger.error("Could not calculate the size of {}.", snapshotDir, e);
            }
        }

        return visitor.getAllocatedSize();
    }

    public Collection<File> getDirectories()
    {
        return snapshotDirs;
    }

    public Optional<File> getManifestFile()
    {
        for (File snapshotDir : snapshotDirs)
        {
            File manifestFile = Directories.getSnapshotManifestFile(snapshotDir);
            if (manifestFile.exists())
            {
                return Optional.of(manifestFile);
            }
        }
        return Optional.empty();
    }

    public Optional<File> getSchemaFile()
    {
        for (File snapshotDir : snapshotDirs)
        {
            File schemaFile = Directories.getSnapshotSchemaFile(snapshotDir);
            if (schemaFile.exists())
            {
                return Optional.of(schemaFile);
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSnapshot snapshot = (TableSnapshot) o;
        return Objects.equals(keyspaceName, snapshot.keyspaceName) && Objects.equals(tableName, snapshot.tableName) &&
               Objects.equals(tableId, snapshot.tableId) && Objects.equals(tag, snapshot.tag) &&
               Objects.equals(createdAt, snapshot.createdAt) && Objects.equals(expiresAt, snapshot.expiresAt) &&
               Objects.equals(snapshotDirs, snapshot.snapshotDirs) && Objects.equals(ephemeral, snapshot.ephemeral);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaceName, tableName, tableId, tag, createdAt, expiresAt, snapshotDirs, ephemeral);
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

    static class Builder {
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

    public static class SnapshotTrueSizeCalculator extends DirectorySizeCalculator
    {
        /**
         * Snapshots are composed of hard-linked sstables. The true snapshot size should only include
         * snapshot files which do not contain a corresponding "live" sstable file.
         */
        @Override
        public boolean isAcceptable(Path snapshotFilePath)
        {
            return !getLiveFileFromSnapshotFile(snapshotFilePath).exists();
        }
    }

    /**
     * Returns the corresponding live file for a given snapshot file.
     *
     * Example:
     *  - Base table:
     *    - Snapshot file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/snapshots/1643481737850/me-1-big-Data.db
     *    - Live file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/me-1-big-Data.db
     *  - Secondary index:
     *    - Snapshot file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/snapshots/1643481737850/.tbl_val_idx/me-1-big-Summary.db
     *    - Live file: ~/.ccm/test/node1/data0/test_ks/tbl-e03faca0813211eca100c705ea09b5ef/.tbl_val_idx/me-1-big-Summary.db
     *
     */
    static File getLiveFileFromSnapshotFile(Path snapshotFilePath)
    {
        // Snapshot directory structure format is {data_dir}/snapshots/{snapshot_name}/{snapshot_file}
        Path liveDir = snapshotFilePath.getParent().getParent().getParent();
        if (Directories.isSecondaryIndexFolder(snapshotFilePath.getParent()))
        {
            // Snapshot file structure format is {data_dir}/snapshots/{snapshot_name}/.{index}/{sstable-component}.db
            liveDir = File.getPath(liveDir.getParent().toString(), snapshotFilePath.getParent().getFileName().toString());
        }
        return new File(liveDir.toString(), snapshotFilePath.getFileName().toString());
    }

    public static Predicate<TableSnapshot> shouldClearSnapshot(String tag, long olderThanTimestamp)
    {
        return ts ->
        {
            // When no tag is supplied, all snapshots must be cleared
            boolean clearAll = tag == null || tag.isEmpty();
            if (!clearAll && ts.isEphemeral())
                logger.info("Skipping deletion of ephemeral snapshot '{}' in keyspace {}. " +
                            "Ephemeral snapshots are not removable by a user.",
                            tag, ts.keyspaceName);
            boolean notEphemeral = !ts.isEphemeral();
            boolean shouldClearTag = clearAll || ts.tag.equals(tag);
            boolean byTimestamp = true;

            if (olderThanTimestamp > 0L)
            {
                Instant createdAt = ts.getCreatedAt();
                if (createdAt != null)
                    byTimestamp = createdAt.isBefore(Instant.ofEpochMilli(olderThanTimestamp));
            }

            return notEphemeral && shouldClearTag && byTimestamp;
        };
    }

}
