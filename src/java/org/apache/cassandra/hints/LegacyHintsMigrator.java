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
package org.apache.cassandra.hints;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A migrator that goes through the legacy system.hints table and writes all the hints to the new hints storage format.
 */
@SuppressWarnings("deprecation")
public final class LegacyHintsMigrator
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyHintsMigrator.class);

    private final File hintsDirectory;
    private final long maxHintsFileSize;

    private final ColumnFamilyStore legacyHintsTable;
    private final int pageSize;

    public LegacyHintsMigrator(File hintsDirectory, long maxHintsFileSize)
    {
        this.hintsDirectory = hintsDirectory;
        this.maxHintsFileSize = maxHintsFileSize;

        legacyHintsTable = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_HINTS);
        pageSize = calculatePageSize(legacyHintsTable);
    }

    // read fewer columns (mutations) per page if they are very large
    private static int calculatePageSize(ColumnFamilyStore legacyHintsTable)
    {
        int size = 128;

        int meanCellCount = legacyHintsTable.getMeanColumns();
        double meanPartitionSize = legacyHintsTable.getMeanPartitionSize();

        if (meanCellCount != 0 && meanPartitionSize != 0)
        {
            int avgHintSize = (int) meanPartitionSize / meanCellCount;
            size = Math.max(2, Math.min(size, (512 << 10) / avgHintSize));
        }

        return size;
    }

    public void migrate()
    {
        // nothing to migrate
        if (legacyHintsTable.isEmpty())
            return;
        logger.info("Migrating legacy hints to new storage");

        // major-compact all of the existing sstables to get rid of the tombstones + expired hints
        logger.info("Forcing a major compaction of {}.{} table", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_HINTS);
        compactLegacyHints();

        // paginate over legacy hints and write them to the new storage
        logger.info("Writing legacy hints to the new storage");
        migrateLegacyHints();

        // truncate the legacy hints table
        logger.info("Truncating {}.{} table", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_HINTS);
        legacyHintsTable.truncateBlocking();
    }

    private void compactLegacyHints()
    {
        Collection<Descriptor> descriptors = new ArrayList<>();
        legacyHintsTable.getTracker().getUncompacting().forEach(sstable -> descriptors.add(sstable.descriptor));
        if (!descriptors.isEmpty())
            forceCompaction(descriptors);
    }

    private void forceCompaction(Collection<Descriptor> descriptors)
    {
        try
        {
            CompactionManager.instance.submitUserDefined(legacyHintsTable, descriptors, FBUtilities.nowInSeconds()).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void migrateLegacyHints()
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);
        String query = String.format("SELECT DISTINCT target_id FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_HINTS);
        //noinspection ConstantConditions
        QueryProcessor.executeInternal(query).forEach(row -> migrateLegacyHints(row.getUUID("target_id"), buffer));
        FileUtils.clean(buffer);
    }

    private void migrateLegacyHints(UUID hostId, ByteBuffer buffer)
    {
        String query = String.format("SELECT target_id, hint_id, message_version, mutation, ttl(mutation) AS ttl, writeTime(mutation) AS write_time " +
                                     "FROM %s.%s " +
                                     "WHERE target_id = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.LEGACY_HINTS);

        // read all the old hints (paged iterator), write them in the new format
        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(query, pageSize, hostId);
        migrateLegacyHints(hostId, rows, buffer);

        // delete the whole partition in the legacy table; we would truncate the whole table afterwards, but this allows
        // to not lose progress in case of a terminated conversion
        deleteLegacyHintsPartition(hostId);
    }

    private void migrateLegacyHints(UUID hostId, UntypedResultSet rows, ByteBuffer buffer)
    {
        migrateLegacyHints(hostId, rows.iterator(), buffer);
    }

    private void migrateLegacyHints(UUID hostId, Iterator<UntypedResultSet.Row> iterator, ByteBuffer buffer)
    {
        do
        {
            migrateLegacyHintsInternal(hostId, iterator, buffer);
            // if there are hints that didn't fit in the previous file, keep calling the method to write to a new
            // file until we get everything written.
        }
        while (iterator.hasNext());
    }

    private void migrateLegacyHintsInternal(UUID hostId, Iterator<UntypedResultSet.Row> iterator, ByteBuffer buffer)
    {
        HintsDescriptor descriptor = new HintsDescriptor(hostId, System.currentTimeMillis());

        try (HintsWriter writer = HintsWriter.create(hintsDirectory, descriptor))
        {
            try (HintsWriter.Session session = writer.newSession(buffer))
            {
                while (iterator.hasNext())
                {
                    Hint hint = convertLegacyHint(iterator.next());
                    if (hint != null)
                        session.append(hint);

                    if (session.position() >= maxHintsFileSize)
                        break;
                }
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, descriptor.fileName());
        }
    }

    private static Hint convertLegacyHint(UntypedResultSet.Row row)
    {
        Mutation mutation = deserializeLegacyMutation(row);
        if (mutation == null)
            return null;

        long creationTime = row.getLong("write_time"); // milliseconds, not micros, for the hints table
        int expirationTime = FBUtilities.nowInSeconds() + row.getInt("ttl");
        int originalGCGS = expirationTime - (int) TimeUnit.MILLISECONDS.toSeconds(creationTime);

        int gcgs = Math.min(originalGCGS, mutation.smallestGCGS());

        return Hint.create(mutation, creationTime, gcgs);
    }

    private static Mutation deserializeLegacyMutation(UntypedResultSet.Row row)
    {
        try (DataInputBuffer dib = new DataInputBuffer(row.getBlob("mutation"), true))
        {
            Mutation mutation = Mutation.serializer.deserialize(dib,
                                                                row.getInt("message_version"));
            mutation.getPartitionUpdates().forEach(PartitionUpdate::validate);
            return mutation;
        }
        catch (IOException e)
        {
            logger.error("Failed to migrate a hint for {} from legacy {}.{} table",
                         row.getUUID("target_id"),
                         SchemaConstants.SYSTEM_KEYSPACE_NAME,
                         SystemKeyspace.LEGACY_HINTS,
                         e);
            return null;
        }
        catch (MarshalException e)
        {
            logger.warn("Failed to validate a hint for {} from legacy {}.{} table - skipping",
                        row.getUUID("target_id"),
                        SchemaConstants.SYSTEM_KEYSPACE_NAME,
                        SystemKeyspace.LEGACY_HINTS,
                        e);
            return null;
        }
    }

    private static void deleteLegacyHintsPartition(UUID hostId)
    {
        // intentionally use millis, like the rest of the legacy implementation did, just in case
        Mutation mutation = new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.LegacyHints,
                                                                             UUIDType.instance.decompose(hostId),
                                                                             System.currentTimeMillis(),
                                                                             FBUtilities.nowInSeconds()));
        mutation.applyUnsafe();
    }
}
