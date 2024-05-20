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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogReader;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.transformations.cms.PreInitialize;

import static org.apache.cassandra.tcm.Epoch.FIRST;

public final class DistributedMetadataLogKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(DistributedMetadataLogKeyspace.class);

    private DistributedMetadataLogKeyspace(){}

    public static final String TABLE_NAME = "distributed_metadata_log";

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 0: original definition in 5.0
     */
    public static final long GENERATION = 0;

    public static final TableId LOG_TABLE_ID = TableId.unsafeDeterministic(SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME);
    public static final String LOG_TABLE_CQL = "CREATE TABLE %s.%s ("
                                               + "epoch bigint,"
                                               + "entry_id bigint,"
                                               + "transformation blob,"
                                               + "kind int,"
                                               + "PRIMARY KEY (epoch))";

    public static final TableMetadata Log =
        parse(LOG_TABLE_CQL, TABLE_NAME, "Log")
        .partitioner(MetaStrategy.partitioner)
        .compaction(CompactionParams.twcs(ImmutableMap.of("compaction_window_unit","DAYS",
                                                          "compaction_window_size","1")))
        .build();

    public static boolean initialize() throws IOException
    {
        try
        {
            String init = String.format("INSERT INTO %s.%s (epoch, transformation, kind, entry_id) " +
                                        "VALUES(?, ?, ?, ?) " +
                                        "IF NOT EXISTS", SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME);
            UntypedResultSet result = QueryProcessor.execute(init, ConsistencyLevel.QUORUM,
                                                             FIRST.getEpoch(),
                                                             Transformation.Kind.PRE_INITIALIZE_CMS.toVersionedBytes(PreInitialize.blank()),
                                                             Transformation.Kind.PRE_INITIALIZE_CMS.id,
                                                             Entry.Id.NONE.entryId);

            UntypedResultSet.Row row = result.one();
            if (row.getBoolean("[applied]"))
                return true;

            if (row.getLong("epoch") == FIRST.getEpoch() &&
                row.getLong("entry_id") == Entry.Id.NONE.entryId &&
                Transformation.Kind.PRE_INITIALIZE_CMS.id == row.getInt("kind"))
                return true;

            throw new IllegalStateException("Could not initialize log.");
        }
        catch (CasWriteTimeoutException t)
        {
            logger.warn("Timed out while trying to CAS", t);
            return false;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Caught an exception while trying to CAS", t);
            return false;
        }
    }

    public static boolean tryCommit(Entry.Id entryId,
                                    Transformation transform,
                                    Epoch previousEpoch,
                                    Epoch nextEpoch)
    {
        try
        {
            if (previousEpoch.is(FIRST) && !initialize())
                return false;

            // TODO get lowest supported metadata version from ClusterMetadata
            ByteBuffer serializedEvent = transform.kind().toVersionedBytes(transform);

            String query = String.format("INSERT INTO %s.%s (epoch, entry_id, transformation, kind) " +
                                         "VALUES (?, ?, ?, ?) " +
                                         "IF NOT EXISTS;",
                                         SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME);
            UntypedResultSet result = QueryProcessor.execute(query,
                                                             ConsistencyLevel.QUORUM,
                                                             nextEpoch.getEpoch(),
                                                             entryId.entryId,
                                                             serializedEvent,
                                                             transform.kind().id);

            return result.one().getBoolean("[applied]");
        }
        catch (CasWriteTimeoutException t)
        {
            logger.warn("Timed out while trying to append item to the log", t);
            return false;
        }
        catch (Throwable t)
        {
            logger.error("Caught an exception while trying to CAS", t);
            return false;
        }
    }

    private static final LogReader localLogReader = new DistributedTableLogReader(ConsistencyLevel.NODE_LOCAL);
    private static final LogReader serialLogReader = new DistributedTableLogReader(ConsistencyLevel.SERIAL);

    public static LogState getLogState(Epoch since, boolean consistentFetch)
    {
        return (consistentFetch ? serialLogReader : localLogReader).getLogState(since);
    }

    public static class DistributedTableLogReader implements LogReader
    {
        private final ConsistencyLevel consistencyLevel;
        private final Supplier<MetadataSnapshots> snapshots;

        public DistributedTableLogReader(ConsistencyLevel consistencyLevel, Supplier<MetadataSnapshots> snapshots)
        {
            this.consistencyLevel = consistencyLevel;
            this.snapshots = snapshots;
        }

        public DistributedTableLogReader(ConsistencyLevel consistencyLevel)
        {
            this(consistencyLevel, () -> ClusterMetadataService.instance().snapshotManager());
        }

        public EntryHolder getEntries(Epoch since) throws IOException
        {
            // during gossip upgrade we have epoch = Long.MIN_VALUE + 1 (and the reverse partitioner doesn't support negative keys)
            since = since.isBefore(Epoch.EMPTY) ? Epoch.EMPTY : since;
            // note that we want all entries with epoch >= since - but since we use a reverse partitioner, we actually
            // want all entries where the token is less than token(since)
            UntypedResultSet resultSet = execute(String.format("SELECT epoch, kind, transformation, entry_id FROM %s.%s WHERE token(epoch) <= token(?)",
                                                               SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME),
                                                 consistencyLevel, since.getEpoch());
            EntryHolder entryHolder = new EntryHolder(since);
            for (UntypedResultSet.Row row : resultSet)
            {
                long entryId = row.getLong("entry_id");
                Epoch epoch = Epoch.create(row.getLong("epoch"));
                Transformation.Kind kind = Transformation.Kind.fromId(row.getInt("kind"));
                Transformation transform = kind.fromVersionedBytes(row.getBlob("transformation"));
                entryHolder.add(new Entry(new Entry.Id(entryId), epoch, transform));
            }
            return entryHolder;
        }

        @Override
        public MetadataSnapshots snapshots()
        {
            return snapshots.get();
        }
    }

    private static UntypedResultSet execute(String query, ConsistencyLevel cl, Object ... params)
    {
        if (cl == ConsistencyLevel.NODE_LOCAL)
            return QueryProcessor.executeInternal(query, params);
        return QueryProcessor.execute(query, cl, params);
    }

    private static TableMetadata.Builder parse(String cql, String table, String description)
    {
        return CreateTableStatement.parse(String.format(cql, SchemaConstants.METADATA_KEYSPACE_NAME, table), SchemaConstants.METADATA_KEYSPACE_NAME)
                                   .id(LOG_TABLE_ID)
                                   .epoch(FIRST)
                                   .comment(description);
    }

    public static KeyspaceMetadata initialMetadata(Set<String> knownDatacenters)
    {
        return KeyspaceMetadata.create(SchemaConstants.METADATA_KEYSPACE_NAME, new KeyspaceParams(true, ReplicationParams.simpleMeta(1, knownDatacenters)), Tables.of(Log));
    }
}