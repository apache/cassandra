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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Clock;

public class TableSnapshot
{
    private static final Logger logger = LoggerFactory.getLogger(TableSnapshot.class);

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

    private final long manifestsSize;
    private final long schemasSize;

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
        catch (IOException t)
        {
            logger.error("Could not list directory content {}", dir);
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
               Objects.equals(tag, snapshot.tag) &&
               Objects.equals(createdAt, snapshot.createdAt) &&
               Objects.equals(expiresAt, snapshot.expiresAt) &&
               Objects.equals(snapshotDirs, snapshot.snapshotDirs) &&
               Objects.equals(ephemeral, snapshot.ephemeral);
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
            maybeCreateOrEnrichManifest();
            return new TableSnapshot(keyspaceName, tableName, tableId, tag, createdAt, expiresAt, snapshotDirs, ephemeral);
        }

        private void maybeCreateOrEnrichManifest()
        {
            // this is caused by not reading any manifest of a new format or that snapshot had none upon loading,
            // that might be the case when upgrading e.g. from 4.0 where basic manifest is created when taking a snapshot,
            // so we just go ahead and enrich it in each snapshot dir
            if (createdAt != null)
                return;

            if (!CassandraRelevantProperties.SNAPSHOT_MANIFEST_ENRICH_ENABLED.getBoolean())
                return;

            createdAt = Instant.ofEpochMilli(Clock.Global.currentTimeMillis());

            List<String> files = new ArrayList<>();
            for (File snapshotDir : snapshotDirs)
            {
                List<File> dataFiles = new ArrayList<>();
                try
                {
                    List<File> indicesDirs = new ArrayList<>();
                    File[] snapshotFiles = snapshotDir.list(file -> {
                        if (file.isDirectory() && file.name().startsWith("."))
                        {
                            indicesDirs.add(file);
                            return false;
                        }
                        else
                        {
                            return file.name().endsWith('-' + SSTableFormat.Components.DATA.type.repr);
                        }
                    });

                    Collections.addAll(dataFiles, snapshotFiles);

                    for (File indexDir : indicesDirs)
                        dataFiles.addAll(Arrays.asList(indexDir.list(file -> file.name().endsWith('-' + SSTableFormat.Components.DATA.type.repr))));

                }
                catch (IOException ex)
                {
                    logger.error("Unable to list a directory for data components: {}", snapshotDir);
                }

                for (File dataFile : dataFiles)
                {
                    Descriptor descriptor = SSTable.tryDescriptorFromFile(dataFile);
                    if (descriptor != null)
                    {
                        String relativeDataFileName = descriptor.relativeFilenameFor(SSTableFormat.Components.DATA);
                        files.add(relativeDataFileName);
                    }
                }
            }

            for (File snapshotDir : snapshotDirs)
            {
                SnapshotManifest snapshotManifest = new SnapshotManifest(files, null, createdAt, ephemeral);
                File manifestFile = new File(snapshotDir, "manifest.json");
                try
                {
                    snapshotManifest.serializeToJsonFile(manifestFile);
                }
                catch (IOException ex)
                {
                    logger.error("Unable to create a manifest.json file in {}", manifestFile.absolutePath());
                }
            }
        }
    }

    protected static String buildSnapshotId(String keyspaceName, String tableName, UUID tableId, String tag)
    {
        return String.format("%s:%s:%s:%s", keyspaceName, tableName, tableId, tag);
    }
}
