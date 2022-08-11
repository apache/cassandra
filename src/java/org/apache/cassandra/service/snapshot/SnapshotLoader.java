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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.db.Directories.SNAPSHOT_SUBDIR;
import static org.apache.cassandra.service.snapshot.TableSnapshot.buildSnapshotId;

/**
 * Loads snapshot metadata from data directories
 */
public class SnapshotLoader extends SimpleFileVisitor<Path>
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotLoader.class);

    static final Pattern SNAPSHOT_DIR_PATTERN = Pattern.compile("(?<keyspace>\\w+)/(?<tableName>\\w+)-(?<tableId>[0-9a-f]{32})/snapshots/(?<tag>.+)$");
    private static final Pattern UUID_PATTERN = Pattern.compile("([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]+)");

    private final Collection<Path> dataDirectories;
    private final Map<String, TableSnapshot.Builder> snapshots = new HashMap<>();

    public SnapshotLoader()
    {
        this(DatabaseDescriptor.getAllDataFileLocations());
    }

    public SnapshotLoader(String[] dataDirectories)
    {
        this(Arrays.stream(dataDirectories).map(Paths::get).collect(Collectors.toList()));
    }

    public SnapshotLoader(Collection<Path> dataDirs)
    {
        this.dataDirectories = dataDirs;
    }

    public SnapshotLoader(Directories directories)
    {
        this(directories.getCFDirectories().stream().map(File::toPath).collect(Collectors.toList()));
    }

    public Set<TableSnapshot> loadSnapshots(String keyspace)
    {
        // if we supply a keyspace, the walking max depth will be suddenly shorther
        // because we are one level down in the directory structure
        int maxDepth = keyspace == null ? 5 : 4;
        for (Path dataDir : dataDirectories)
        {
            if (keyspace != null)
                dataDir = dataDir.resolve(keyspace);

            try
            {
                if (new File(dataDir).exists())
                {
                    Files.walkFileTree(dataDir, Collections.emptySet(), maxDepth, this);
                }
                else
                {
                    logger.debug("Skipping non-existing data directory {}", dataDir);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(String.format("Error while loading snapshots from %s", dataDir), e);
            }
        }
        return snapshots.values().stream().map(TableSnapshot.Builder::build).collect(Collectors.toSet());
    }

    public Set<TableSnapshot> loadSnapshots()
    {
        return loadSnapshots(null);
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        // Cassandra can remove some files while traversing the tree,
        // for example when SSTables are compacted while we are walking it.
        // SnapshotLoader is interested only in SSTables in snapshot directories which are not compacted,
        // but we need to cover these in regular table directories too.
        // If listing failed but such file exists and the exception is not NoSuchFileException, then we
        // have a legitimate error while traversing the tree, otherwise just skip it and continue with the listing.
        if (Files.exists(file) && !(exc instanceof NoSuchFileException))
            return super.visitFileFailed(file, exc);
        else
            return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path subdir, BasicFileAttributes attrs)
    {
        if (subdir.getParent().getFileName().toString().equals(SNAPSHOT_SUBDIR))
        {
            logger.trace("Processing directory " + subdir);
            Matcher snapshotDirMatcher = SNAPSHOT_DIR_PATTERN.matcher(subdir.toString());
            if (snapshotDirMatcher.find())
            {
                try
                {
                    loadSnapshotFromDir(snapshotDirMatcher, subdir);
                } catch (Throwable e)
                {
                    logger.warn("Could not load snapshot from {}.", subdir, e);
                }
            }
            return FileVisitResult.SKIP_SUBTREE;
        }

        return subdir.getFileName().toString().equals(Directories.BACKUPS_SUBDIR)
               ? FileVisitResult.SKIP_SUBTREE
               : FileVisitResult.CONTINUE;
    }

    private void loadSnapshotFromDir(Matcher snapshotDirMatcher, Path snapshotDir)
    {
        String keyspaceName = snapshotDirMatcher.group("keyspace");
        String tableName = snapshotDirMatcher.group("tableName");
        UUID tableId = parseUUID(snapshotDirMatcher.group("tableId"));
        String tag = snapshotDirMatcher.group("tag");
        String snapshotId = buildSnapshotId(keyspaceName, tableName, tableId, tag);
        TableSnapshot.Builder builder = snapshots.computeIfAbsent(snapshotId, k -> new TableSnapshot.Builder(keyspaceName, tableName, tableId, tag));
        builder.addSnapshotDir(new File(snapshotDir));
    }

    /**
     * Given an UUID string without dashes (ie. c7e513243f0711ec9bbc0242ac130002)
     * return an UUID object (ie. c7e51324-3f07-11ec-9bbc-0242ac130002)
     */
    protected static UUID parseUUID(String uuidWithoutDashes) throws IllegalArgumentException
    {
        assert uuidWithoutDashes.length() == 32 && !uuidWithoutDashes.contains("-");
        String dashedUUID = UUID_PATTERN.matcher(uuidWithoutDashes).replaceFirst("$1-$2-$3-$4-$5");
        return UUID.fromString(dashedUUID);
    }
}
