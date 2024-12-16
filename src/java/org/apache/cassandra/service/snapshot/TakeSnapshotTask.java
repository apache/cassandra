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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

public class TakeSnapshotTask extends AbstractSnapshotTask<List<TableSnapshot>>
{
    private static final Logger logger = LoggerFactory.getLogger(TakeSnapshotTask.class);
    private final SnapshotManager manager;

    private Instant creationTime;
    private String snapshotName;
    private final Map<ColumnFamilyStore, TableSnapshot> snapshotsToCreate = new HashMap<>();

    public TakeSnapshotTask(SnapshotManager manager, SnapshotOptions options)
    {
        super(options);
        this.manager = manager;
    }

    @Override
    public SnapshotTaskType getTaskType()
    {
        return SnapshotTaskType.SNAPSHOT;
    }

    public Map<ColumnFamilyStore, TableSnapshot> getSnapshotsToCreate()
    {
        if (StorageService.instance.operationMode() == StorageService.Mode.JOINING)
            throw new RuntimeException("Cannot snapshot until bootstrap completes");

        // This is not in builder's build method on purpose in order to postpone the timestamp for as long as possible
        // until the actual snapshot is taken. If we constructed a task and have not done anything with it for 5 minutes
        // then by the time a snapshot would be taken the creation time would be quite off
        Instant creationTimeInOptions = options.creationTime;
        if (creationTimeInOptions == null)
            creationTime = Instant.ofEpochMilli(Clock.Global.currentTimeMillis());
        else
            creationTime = options.creationTime;

        snapshotName = options.getSnapshotName(creationTime);

        Set<ColumnFamilyStore> entitiesForSnapshot = options.cfs == null ? parseEntitiesForSnapshot(options.entities) : Set.of(options.cfs);

        for (ColumnFamilyStore cfs : entitiesForSnapshot)
        {
            Set<File> snapshotDirs = cfs.getDirectories().getSnapshotDirsWithoutCreation(snapshotName);

            TableSnapshot tableSnapshot = new TableSnapshot(cfs.metadata.keyspace,
                                                            cfs.metadata.name,
                                                            cfs.metadata.id.asUUID(),
                                                            snapshotName,
                                                            creationTime,
                                                            SnapshotManifest.computeExpiration(options.ttl, creationTime),
                                                            snapshotDirs,
                                                            options.ephemeral);
            // this snapshot does not have any actual representation on disk
            // because that snapshot was technically not taken yet
            tableSnapshot.incomplete();
            snapshotsToCreate.put(cfs, tableSnapshot);
        }

        return snapshotsToCreate;
    }

    @Override
    public List<TableSnapshot> call()
    {
        assert snapshotName != null : "You need to call getSnapshotsToCreate() first";

        List<TableSnapshot> createdSnapshots = new ArrayList<>();

        for (Map.Entry<ColumnFamilyStore, TableSnapshot> entry : snapshotsToCreate.entrySet())
        {
            try
            {
                ColumnFamilyStore cfs = entry.getKey();
                if (!options.skipFlush)
                {
                    Memtable current = cfs.getTracker().getView().getCurrentMemtable();
                    if (!current.isClean())
                    {
                        if (current.shouldSwitch(ColumnFamilyStore.FlushReason.SNAPSHOT))
                            FBUtilities.waitOnFuture(cfs.switchMemtableIfCurrent(current, ColumnFamilyStore.FlushReason.SNAPSHOT));
                        else
                            current.performSnapshot(snapshotName);
                    }
                }

                createSnapshot(cfs, entry.getValue(), snapshotName, creationTime);
                createdSnapshots.add(entry.getValue());
            }
            catch (Throwable t)
            {
                logger.warn(String.format("Unable to create snapshot %s for %s", entry.getValue().getTag(), entry.getKey().getKeyspaceTableName()), t);
                // if we fail to take a snapshot, there is its phantom / in-progress representation among manager's taken snapshots,
                // so we need to remove it from there to not appear in the outputs / not leak
                manager.getSnapshots().remove(entry.getValue());
            }
        }

        return createdSnapshots;
    }

    private void createSnapshot(ColumnFamilyStore cfs, TableSnapshot snapshotToCreate, String snapshotName, Instant creationTime)
    {
        Predicate<SSTableReader> predicate = options.sstableFilter;
        Set<SSTableReader> sstables = new LinkedHashSet<>();
        for (ColumnFamilyStore aCfs : cfs.concatWithIndexes())
        {
            try (ColumnFamilyStore.RefViewFragment currentView = aCfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> predicate == null || predicate.test(x))))
            {
                for (SSTableReader ssTable : currentView.sstables)
                {
                    File snapshotDirectory = Directories.getSnapshotDirectory(ssTable.descriptor, snapshotName);
                    ssTable.createLinks(snapshotDirectory.path(), options.rateLimiter); // hard links
                    if (logger.isTraceEnabled())
                        logger.trace("Snapshot for {} keyspace data file {} created in {}", cfs.keyspace, ssTable.getFilename(), snapshotDirectory);
                    sstables.add(ssTable);
                }
            }
        }

        List<String> dataComponents = new ArrayList<>();
        for (SSTableReader sstable : sstables)
            dataComponents.add(sstable.descriptor.relativeFilenameFor(SSTableFormat.Components.DATA));

        SnapshotManifest manifest = new SnapshotManifest(dataComponents, options.ttl, creationTime, options.ephemeral);
        Set<File> snapshotDirs = cfs.getDirectories().getSnapshotDirs(snapshotName);
        for (File snapshotDir : snapshotDirs)
        {
            writeSnapshotManifest(manifest, Directories.getSnapshotManifestFile(snapshotDir));

            if (!SchemaConstants.isLocalSystemKeyspace(cfs.metadata.keyspace)
                && !SchemaConstants.isReplicatedSystemKeyspace(cfs.metadata.keyspace))
            {
                writeSnapshotSchema(Directories.getSnapshotSchemaFile(snapshotDir), cfs);
            }
        }

        snapshotToCreate.updateMetadataSize();
        snapshotToCreate.complete();
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
                // but a snapshot is apparently taken before a secondary index is scrubbed,
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

    @Override
    public String toString()
    {
        return "TakeSnapshotTask{" +
               "options=" + options +
               '}';
    }
}
