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
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StartupChecks;

public class TableSnapshot
{
    private static final Logger logger = LoggerFactory.getLogger(StartupChecks.class);

    private final String keyspace;
    private final String table;
    private final String tag;

    private final Instant createdAt;
    private final Instant expiresAt;

    private final Set<File> snapshotDirs;
    private final Function<File, Long> trueDiskSizeComputer;

    public TableSnapshot(String keyspace, String table, String tag, Instant createdAt,
                         Instant expiresAt, Set<File> snapshotDirs,
                         Function<File, Long> trueDiskSizeComputer)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tag = tag;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.snapshotDirs = snapshotDirs;
        this.trueDiskSizeComputer = trueDiskSizeComputer;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
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
        return snapshotDirs.stream().mapToLong(trueDiskSizeComputer::apply).sum();
    }

    public Collection<File> getDirectories()
    {
        return snapshotDirs;
    }

    @Override
    public String toString()
    {
        return "TableSnapshot{" +
               "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", tag='" + tag + '\'' +
               ", createdAt=" + createdAt +
               ", expiresAt=" + expiresAt +
               ", snapshotDirs=" + snapshotDirs +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSnapshot that = (TableSnapshot) o;
        return Objects.equals(keyspace, that.keyspace) && Objects.equals(table, that.table)
               && Objects.equals(tag, that.tag) && Objects.equals(createdAt, that.createdAt)
               && Objects.equals(expiresAt, that.expiresAt) && Objects.equals(snapshotDirs, that.snapshotDirs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, table, tag, createdAt, expiresAt, snapshotDirs);
    }

    public static Map<String, TableSnapshot> filter(Map<String, TableSnapshot> snapshots, Map<String, String> options)
    {
        if (options == null)
            return snapshots;

        boolean skipExpiring = Boolean.parseBoolean(options.getOrDefault("no_ttl", "false"));

        return snapshots.entrySet()
                        .stream()
                        .filter(entry -> !skipExpiring || !entry.getValue().isExpiring())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static class Builder {
        private final String keyspace;
        private final String table;
        private final String tag;

        private Instant createdAt = null;
        private Instant expiresAt = null;

        private final Set<File> snapshotDirs = new HashSet<>();

        Builder(String keyspace, String table, String tag)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.tag = tag;
        }

        void addSnapshotDir(File snapshotDir)
        {
            snapshotDirs.add(snapshotDir);
            File manifestFile = new File(snapshotDir, "manifest.json");
            if (manifestFile.exists() && createdAt == null && expiresAt == null) {
                loadTimestampsFromManifest(manifestFile);
            }
        }

        private void loadTimestampsFromManifest(File manifestFile)
        {
            try
            {
                logger.info("Loading manifest from {}", manifestFile);
                SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);
                createdAt = manifest.createdAt;
                expiresAt = manifest.expiresAt;
            }
            catch (IOException e)
            {
                logger.warn("Cannot read manifest file {} of snapshot {}.", manifestFile, tag, e);
            }
        }

        TableSnapshot build()
        {
            return new TableSnapshot(keyspace, table, tag, createdAt, expiresAt, snapshotDirs, null);
        }
    }
}
