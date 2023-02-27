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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Period;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogReader;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.Replication;
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

    public static final String LOG_TABLE_CQL = "CREATE TABLE %s.%s ("
                                               + "period bigint,"
                                               + "current_epoch bigint static,"
                                               + "sealed boolean static,"
                                               + "epoch bigint,"
                                               + "entry_id bigint,"
                                               + "transformation blob,"
                                               + "kind text,"
                                               + "PRIMARY KEY (period, epoch))";

    public static final TableMetadata Log =
        parse(LOG_TABLE_CQL, TABLE_NAME, "Log")
        .compaction(CompactionParams.twcs(ImmutableMap.of("compaction_window_unit","DAYS",
                                                          "compaction_window_size","1")))
        .build();

    public static boolean initialize() throws IOException
    {
        try
        {
            String init = String.format("INSERT INTO %s.%s (period, epoch, current_epoch, transformation, kind, entry_id, sealed) VALUES(?, ?, ?, ?, ?, ?, false) IF NOT EXISTS", SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME);
            UntypedResultSet result = QueryProcessor.execute(init, ConsistencyLevel.QUORUM,
                                                             Period.FIRST, FIRST.getEpoch(), FIRST.getEpoch(),
                                                             Transformation.Kind.PRE_INITIALIZE_CMS.toVersionedBytes(PreInitialize.blank()), Transformation.Kind.PRE_INITIALIZE_CMS.toString(), Entry.Id.NONE.entryId);

            return result.one().getBoolean("[applied]");
        }
        catch (Throwable t)
        {
            logger.error("Caught an exception while trying to CAS", t);
            return false;
        }
    }

    public static boolean tryCommit(Entry.Id entryId,
                                    Transformation transform,
                                    Epoch previousEpoch,
                                    Epoch nextEpoch,
                                    long previousPeriod,
                                    long nextPeriod,
                                    boolean sealCurrentPeriod)
    {
        try
        {
            if (previousEpoch.is(FIRST) && !initialize())
                return false;

            // TODO get lowest supported metadata version from ClusterMetadata
            ByteBuffer serializedEvent = transform.kind().toVersionedBytes(transform);

            UntypedResultSet result;
            if (previousPeriod + 1 == nextPeriod)
            {
                String query = String.format("BEGIN BATCH\n" +
                                             "INSERT INTO %s.%s (period, epoch, current_epoch, entry_id, transformation, kind, sealed) VALUES (?, ?, ?, ?, ?, ?, false) IF NOT EXISTS;\n" +
                                             "APPLY BATCH",
                                             SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME);
                result = QueryProcessor.execute(query, ConsistencyLevel.QUORUM,
                                                nextPeriod, nextEpoch.getEpoch(), nextEpoch.getEpoch(), entryId.entryId, serializedEvent, transform.kind().toString());
            }
            else
            {
                assert previousPeriod == nextPeriod;
                String query = String.format("BEGIN BATCH\n" +
                                             "UPDATE %s.%s SET current_epoch = ?, sealed = ? WHERE period = ? IF current_epoch = ? AND sealed = false;\n" +
                                             "UPDATE %s.%s SET entry_id = ?, transformation = ?, kind = ? WHERE period = ? AND epoch = ? IF current_epoch = ? and sealed = false;\n" +
                                             "APPLY BATCH",
                                             SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME,
                                             SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME);

                result = QueryProcessor.execute(query,
                                                ConsistencyLevel.QUORUM,
                                                nextEpoch.getEpoch(), sealCurrentPeriod, previousPeriod, previousEpoch.getEpoch(),
                                                entryId.entryId, serializedEvent, transform.kind().toString(),
                                                previousPeriod, nextEpoch.getEpoch(), previousEpoch.getEpoch());
            }

            return result.one().getBoolean("[applied]");
        }
        catch (Throwable t)
        {
            logger.error("Caught an exception while trying to CAS", t);
            return false;
        }
    }

    @VisibleForTesting
    public static void truncateLogState()
    {
        QueryProcessor.execute(String.format("TRUNCATE %s.%s", SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME), ConsistencyLevel.QUORUM);
    }

    // supplies Replication for LogState and catchup
    public static final LogReader logReader = new DistributedLogReader();

    public static LogState getLogState(Epoch since)
    {
        return getLogState(since, ClusterMetadataService.instance().snapshotManager());
    }

    @VisibleForTesting
    public static LogState getLogState(Epoch since, MetadataSnapshots snapshots)
    {
        return LogState.getLogState(since, snapshots, logReader);
    }

    private static class DistributedLogReader implements LogReader
    {
        @Override
        public Replication getReplication(long startPeriod, Epoch since)
        {
            try
            {
                if (startPeriod == Period.EMPTY)
                {
                    startPeriod = Period.scanLogForPeriod(Log, since);
                    if (startPeriod == Period.EMPTY)
                        return Replication.EMPTY;
                }

                long currentEpoch = since.getEpoch();
                long lastEpoch = since.getEpoch();

                long period = startPeriod;
                ImmutableList.Builder<Entry> entries = new ImmutableList.Builder<>();

                while (true)
                {
                    boolean empty = true;
                    UntypedResultSet resultSet = execute(String.format("SELECT current_epoch, period, epoch, kind, transformation, entry_id, sealed FROM %s.%s WHERE period = ? AND epoch > ?",
                                                          SchemaConstants.METADATA_KEYSPACE_NAME, TABLE_NAME),
                                                         ConsistencyLevel.SERIAL, period, since.getEpoch());

                    // TODO: timeout here causes cluster to stall
                    for (UntypedResultSet.Row row : resultSet)
                    {
                        currentEpoch = row.getLong("current_epoch");
                        long epochl = row.getLong("epoch");
                        Epoch epoch = Epoch.create(epochl);
                        Transformation.Kind kind = Transformation.Kind.valueOf(row.getString("kind"));
                        long entryId = row.getLong("entry_id");
                        Transformation transform = kind.fromVersionedBytes(row.getBlob("transformation"));
                        entries.add(new Entry(new Entry.Id(entryId), epoch, transform));

                        lastEpoch = currentEpoch;
                        empty = false;
                    }

                    if (period != startPeriod && empty)
                        break;

                    period++;
                }

                assert currentEpoch == lastEpoch;
                return new Replication(entries.build());
            }
            catch (Throwable t)
            {
                logger.error("Was not able to query the cluster metadata state", t);
                throw new RuntimeException(t);
            }
        }
    };

    private static UntypedResultSet execute(String query, ConsistencyLevel cl, Object ... params)
    {
        return QueryProcessor.execute(query, cl, params);
    }

    private static TableMetadata.Builder parse(String cql, String table, String description)
    {
        return CreateTableStatement.parse(String.format(cql, SchemaConstants.METADATA_KEYSPACE_NAME, table), SchemaConstants.METADATA_KEYSPACE_NAME)
                                   .id(TableId.unsafeDeterministic(SchemaConstants.METADATA_KEYSPACE_NAME, table))
                                   .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.METADATA_KEYSPACE_NAME, new KeyspaceParams(true, ReplicationParams.meta()), Tables.of(Log));
    }
}