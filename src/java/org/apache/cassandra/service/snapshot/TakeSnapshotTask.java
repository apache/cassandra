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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.base.Predicate;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec.IntSecondsBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;

public class TakeSnapshotTask implements Callable<List<TableSnapshot>>
{
    public static final String SKIP_FLUSH = "skipFlush";
    public static final String TTL = "ttl";

    private static final Logger logger = LoggerFactory.getLogger(TakeSnapshotTask.class);

    private final String tag;
    private IntSecondsBound ttl;
    private Instant creationTime;
    private boolean skipFlush;
    private boolean ephemeral;
    private final Predicate<SSTableReader> predicate;
    private final RateLimiter rateLimiter;
    private final ColumnFamilyStore cfs;
    private final String[] entities;

    private TakeSnapshotTask(String tag,
                             IntSecondsBound ttl,
                             Instant creationTime,
                             boolean skipFlush,
                             boolean ephemeral,
                             Predicate<SSTableReader> predicate,
                             RateLimiter rateLimiter,
                             ColumnFamilyStore cfs,
                             String... entities)
    {
        this.tag = tag;
        this.ttl = ttl;
        this.creationTime = creationTime;
        this.skipFlush = skipFlush;
        this.ephemeral = ephemeral;
        this.predicate = predicate;
        this.rateLimiter = rateLimiter == null ? DatabaseDescriptor.getSnapshotRateLimiter() : rateLimiter;
        this.cfs = cfs;
        this.entities = entities;
    }

    public static class Builder
    {
        private String tag;
        private IntSecondsBound ttl;
        private Instant creationTime;
        private boolean skipFlush = false;
        private boolean ephemeral = false;
        private Predicate<SSTableReader> predicate;
        private RateLimiter rateLimiter;
        private String[] entities;
        private ColumnFamilyStore cfs;

        public Builder(String tag, String... entities)
        {
            this.tag = tag;
            this.entities = entities;
        }

        public Builder predicate(Predicate<SSTableReader> predicate)
        {
            this.predicate = predicate;
            return this;
        }

        public Builder rateLimiter(RateLimiter rateLimiter)
        {
            this.rateLimiter = rateLimiter;
            return this;
        }

        public Builder ttl(String ttl)
        {
            if (ttl != null)
                this.ttl = new IntSecondsBound(ttl);

            return this;
        }

        public Builder ttl(IntSecondsBound ttl)
        {
            this.ttl = ttl;
            return this;
        }

        public Builder creationTime(String creationTime)
        {
            if (creationTime != null)
            {
                try
                {
                    this.creationTime = Instant.ofEpochMilli(Long.parseLong(creationTime));
                }
                catch (Exception ex)
                {
                    throw new RuntimeException("Unable to parse creation time from " + creationTime);
                }
            }

            return this;
        }

        public Builder creationTime(Instant creationTime)
        {
            this.creationTime = creationTime;
            return this;
        }

        public Builder skipFlush()
        {
            skipFlush = true;
            return this;
        }

        public Builder ephemeral()
        {
            ephemeral = true;
            return this;
        }

        public Builder cfs(ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
            return this;
        }

        public TakeSnapshotTask build()
        {
            if (tag == null || tag.isEmpty())
                throw new RuntimeException("You must supply a snapshot name.");

            if (ttl != null)
            {
                int minAllowedTtlSecs = CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS.getInt();
                if (ttl.toSeconds() < minAllowedTtlSecs)
                    throw new IllegalArgumentException(format("ttl for snapshot must be at least %d seconds", minAllowedTtlSecs));
            }

            if (ephemeral && ttl != null)
                throw new IllegalStateException(format("can not take ephemeral snapshot (%s) while ttl is specified too", tag));

            return new TakeSnapshotTask(tag, ttl, creationTime, skipFlush, ephemeral, predicate, rateLimiter, cfs, entities);
        }
    }

    @Override
    public List<TableSnapshot> call()
    {
        if (StorageService.instance.operationMode() == StorageService.Mode.JOINING)
            throw new RuntimeException("Cannot snapshot until bootstrap completes");

        Set<ColumnFamilyStore> entitiesForSnapshot = cfs == null ? parseEntitiesForSnapshot(entities) : Set.of(cfs);

        for (ColumnFamilyStore table : entitiesForSnapshot)
        {
            String keyspaceName = table.getKeyspaceName();
            String tableName = table.getTableName();
            if (SnapshotManager.instance.getSnapshot(table.getKeyspaceName(), table.getTableName(), tag).isPresent())
                throw new RuntimeException(format("Snapshot %s for %s.%s already exists.", tag, keyspaceName, tableName));
        }

        List<TableSnapshot> snapshots = new LinkedList<>();

        // This is not in builder's build method on purpose in order to postpone the timestamp for as long as possible
        // until the actual snapshot is taken. If we constructed a task and have not done anything with it for 5 minutes
        // then by the time a snapshot would be taken the creation time would be quite off
        if (creationTime == null)
            creationTime = FBUtilities.now();

        for (ColumnFamilyStore cfs : entitiesForSnapshot)
        {
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

            TableSnapshot snapshot = createSnapshot(cfs, tag, predicate, ephemeral, ttl, creationTime);
            snapshots.add(snapshot);
        }

        return snapshots;
    }

    private TableSnapshot createSnapshot(ColumnFamilyStore cfs,
                                         String tag,
                                         Predicate<SSTableReader> predicate,
                                         boolean ephemeral,
                                         IntSecondsBound ttl,
                                         Instant creationTime)
    {
        Set<SSTableReader> sstables = new LinkedHashSet<>();
        for (ColumnFamilyStore aCfs : cfs.concatWithIndexes())
        {
            try (ColumnFamilyStore.RefViewFragment currentView = aCfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.apply(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, tag);
                    ssTable.createLinks(snapshotDirectory.path(), rateLimiter); // hard links
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

    private Set<ColumnFamilyStore> parseEntitiesForSnapshot(String... entities)
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
                        throw new RuntimeException("You must supply a keyspace name");
                    if (tableName == null)
                        throw new RuntimeException("You must supply a table name");

                    Keyspace validKeyspace = Keyspace.getValidKeyspace(keyspaceName);
                    ColumnFamilyStore existingTable = validKeyspace.getColumnFamilyStore(tableName);

                    entitiesForSnapshot.add(existingTable);
                }
                // special case for index which we can not normally create a snapshot for
                // but a snapshot is apparently taken before a secondary index is scrubbed
                // so we preserve this behavior
                else if (splitted.length == 3)
                {
                    String keyspaceName = splitted[0];
                    String tableName = splitted[1];

                    Keyspace validKeyspace = Keyspace.getValidKeyspace(keyspaceName);
                    ColumnFamilyStore existingTable = validKeyspace.getColumnFamilyStore(tableName);
                    Index indexByName = existingTable.indexManager.getIndexByName(splitted[2]);
                    if (indexByName instanceof CassandraIndex)
                    {
                        ColumnFamilyStore indexCfs = ((CassandraIndex) indexByName).getIndexCfs();
                        entitiesForSnapshot.add(indexCfs);
                    }
                    else
                    {
                        throw new IllegalArgumentException("Unknown index " + entity);
                    }
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
