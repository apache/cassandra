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
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.base.Predicate;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.utils.FBUtilities.now;

public class TakeSnapshotTask implements Callable<Set<TableSnapshot>>
{
    private static final Logger logger = LoggerFactory.getLogger(TakeSnapshotTask.class);

    private final String tag;
    private final Map<String, String> options;
    private Predicate<SSTableReader> predicate;
    private final String[] entities;

    public TakeSnapshotTask(String tag,
                            Map<String, String> options,
                            Predicate<SSTableReader> predicate,
                            String... entities)
    {
        this.tag = tag;
        this.options = options;
        this.predicate = predicate;
        this.entities = entities;
    }

    public TakeSnapshotTask(String tag,
                            Map<String, String> options,
                            String... entities)
    {
        this(tag, options, null, entities);
    }

    @Override
    public Set<TableSnapshot> call() throws IOException
    {
        if (StorageService.instance.operationMode() == StorageService.Mode.JOINING)
            throw new IOException("Cannot snapshot until bootstrap completes");

        if (tag == null || tag.isEmpty())
            throw new IOException("You must supply a snapshot name.");

        DurationSpec.IntSecondsBound ttl = options.containsKey("ttl") ? new DurationSpec.IntSecondsBound(options.get("ttl")) : null;
        if (ttl != null)
        {
            int minAllowedTtlSecs = CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS.getInt();
            if (ttl.toSeconds() < minAllowedTtlSecs)
                throw new IllegalArgumentException(format("ttl for snapshot must be at least %d seconds", minAllowedTtlSecs));
        }

        boolean skipFlush = Boolean.parseBoolean(options.getOrDefault("skipFlush", "false"));

        Set<ColumnFamilyStore> entitiesForSnapshot = parseEntitiesForSnapshot(entities);

        for (ColumnFamilyStore table : entitiesForSnapshot)
        {
            String keyspaceName = table.getKeyspaceName();
            String tableName = table.getTableName();
            if (SnapshotManager.instance.getSnapshot(table.getKeyspaceName(), table.getTableName(), tag).isPresent())
                throw new IOException(format("Snapshot %s for %s.%s already exists.", tag, keyspaceName, tableName));
        }

        Instant creationTime = now();

        Set<TableSnapshot> snapshots = new HashSet<>();

        for (ColumnFamilyStore cfs : entitiesForSnapshot)
        {
            // predicate null
            // ephemeral false
            // skipMemtable = skipFlush
            // TODO probably double check that cfs still exists at this point
            if (!skipFlush)
            {
                Memtable current = cfs.getTracker().getView().getCurrentMemtable();
                if (!current.isClean())
                {
                    if (current.shouldSwitch(ColumnFamilyStore.FlushReason.SNAPSHOT))
                        FBUtilities.waitOnFuture(cfs.switchMemtableIfCurrent(current, ColumnFamilyStore.FlushReason.SNAPSHOT));
                    else
                        current.performSnapshot(tag);
                }
            }

            TableSnapshot snapshot = createSnapshot(cfs, tag, predicate, false, ttl, creationTime, SnapshotManager.instance.snapshotRateLimiter);
            snapshots.add(snapshot);
        }

        return snapshots;
    }

    public TableSnapshot createSnapshot(ColumnFamilyStore cfs,
                                        String tag,
                                        Predicate<SSTableReader> predicate,
                                        boolean ephemeral,
                                        DurationSpec.IntSecondsBound ttl,
                                        Instant creationTime,
                                        RateLimiter rateLimiter)
    {
        if (ephemeral && ttl != null)
            throw new IllegalStateException(format("can not take ephemeral snapshot (%s) while ttl is specified too", tag));

        RateLimiter limiter = rateLimiter;
        if (limiter == null)
            limiter = SnapshotManager.instance.snapshotRateLimiter;

        Set<SSTableReader> sstables = new LinkedHashSet<>();
        for (ColumnFamilyStore aCfs : cfs.concatWithIndexes())
        {
            try (ColumnFamilyStore.RefViewFragment currentView = aCfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, tag);
                    ssTable.createLinks(snapshotDirectory.path(), limiter); // hard links
                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", cfs.keyspace, ssTable.getFilename(), snapshotDirectory);
                    sstables.add(ssTable);
                }
            }
        }

        List<String> dataComponents = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            dataComponents.add(sstable.descriptor.relativeFilenameFor(SSTableFormat.Components.DATA));

        SnapshotManifest manifest = new SnapshotManifest(dataComponents, ttl, creationTime, ephemeral);

        Set<File> snapshotDirs = cfs.getDirectories().getSnapshotDirs(tag);

        for (File snapshotDir : snapshotDirs)
        {
            writeSnapshotManifest(manifest, Directories.getSnapshotManifestFile(snapshotDir));

            if (!SchemaConstants.isLocalSystemKeyspace(cfs.metadata.keyspace)
                && !SchemaConstants.isReplicatedSystemKeyspace(cfs.metadata.keyspace))
            {
                writeSnapshotSchema(Directories.getSnapshotSchemaFile(snapshotDir), cfs);
            }
        }

        return new TableSnapshot(cfs.metadata.keyspace,
                                 cfs.metadata.name,
                                 cfs.metadata.id.asUUID(),
                                 tag,
                                 creationTime,
                                 SnapshotManifest.computeExpiration(ttl, creationTime),
                                 snapshotDirs,
                                 ephemeral);
    }

    private Set<ColumnFamilyStore> parseEntitiesForSnapshot(String... entities) throws IOException
    {
        Set<ColumnFamilyStore> entitiesForSnapshot = new HashSet<>();

        if (entities != null && entities.length > 0 && entities[0].contains("."))
        {
            for (String entity : entities)
            {
                String[] splitted = StringUtils.split(entity, '.');
                if (splitted.length == 2)
                {
                    String keyspaceName = splitted[0];
                    String tableName = splitted[1];

                    if (keyspaceName == null)
                        throw new IOException("You must supply a keyspace name");
                    if (tableName == null)
                        throw new IOException("You must supply a table name");

                    Keyspace validKeyspace = Keyspace.getValidKeyspace(keyspaceName);
                    ColumnFamilyStore existingTable = validKeyspace.getColumnFamilyStore(tableName);

                    entitiesForSnapshot.add(existingTable);
                }
                else
                {
                    throw new IllegalArgumentException("Cannot take a snapshot on secondary index or invalid column " +
                                                       "family name. You must supply a column family name in the " +
                                                       "form of keyspace.columnfamily");
                }
            }
        }
        else
        {
            if (entities != null && entities.length == 0)
            {
                for (Keyspace keyspace : Keyspace.all())
                {
                    entitiesForSnapshot.addAll(keyspace.getColumnFamilyStores());
                }
            }
            else if (entities != null)
            {
                for (String keyspace : entities)
                {
                    Keyspace validKeyspace = Keyspace.getValidKeyspace(keyspace);
                    entitiesForSnapshot.addAll(validKeyspace.getColumnFamilyStores());
                }
            }
        }

        return entitiesForSnapshot;
    }


    private void writeSnapshotManifest(SnapshotManifest manifest, File manifestFile)
    {
        try
        {
            manifestFile.parent().tryCreateDirectories();
            manifest.serializeToJsonFile(manifestFile);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, manifestFile);
        }
    }

    private void writeSnapshotSchema(File schemaFile, ColumnFamilyStore cfs)
    {
        try
        {
            if (!schemaFile.parent().exists())
                schemaFile.parent().tryCreateDirectories();

            try (PrintStream out = new PrintStream(new FileOutputStreamPlus(schemaFile)))
            {
                SchemaCQLHelper.reCreateStatementsForSchemaCql(cfs.metadata(), cfs.keyspace.getMetadata())
                               .forEach(out::println);
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, schemaFile);
        }
    }
}
