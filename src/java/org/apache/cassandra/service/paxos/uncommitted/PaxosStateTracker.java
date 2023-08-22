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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.FORCE_PAXOS_STATE_REBUILD;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_PAXOS_STATE_REBUILD;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRUNCATE_BALLOT_METADATA;
import static org.apache.cassandra.db.SystemKeyspace.PAXOS_REPAIR_HISTORY;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;

/**
 * Tracks uncommitted and ballot high/low bounds
 */
public class PaxosStateTracker
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosStateTracker.class);

    /** when starting with no data, skip rebuilding uncommitted data from the paxos table. */
    private static boolean skipRebuild()
    {
        return SKIP_PAXOS_STATE_REBUILD.getBoolean();
    }

    private static boolean forceRebuild()
    {
        return FORCE_PAXOS_STATE_REBUILD.getBoolean();
    }

    private static boolean truncateBallotMetadata()
    {
        return TRUNCATE_BALLOT_METADATA.getBoolean();
    }

    private static final String DIRECTORY = "system/" + SystemKeyspace.PAXOS_REPAIR_STATE;

    private final PaxosUncommittedTracker uncommitted;
    private final PaxosBallotTracker ballots;
    private boolean rebuildNeeded;

    public PaxosStateTracker(PaxosUncommittedTracker uncommitted, PaxosBallotTracker ballots, boolean rebuildNeeded)
    {
        this.uncommitted = uncommitted;
        this.ballots = ballots;
        this.rebuildNeeded = rebuildNeeded;
    }

    public boolean isRebuildNeeded()
    {
        return rebuildNeeded;
    }

    static File stateDirectory(File dataDirectory)
    {
        return new File(dataDirectory, DIRECTORY);
    }

    public static PaxosStateTracker create(File[] directories) throws IOException
    {
        File stateDirectory = null;
        boolean hasExistingData = false;

        for (File directory : directories)
        {
            File candidate = stateDirectory(directory);
            if (candidate.exists() && new File(candidate, PaxosBallotTracker.FNAME).exists())
            {
                Preconditions.checkState(!hasExistingData,
                                         "Multiple paxos repair metadata directories found (%s, %s), remove the older directory and restart.",
                                         stateDirectory, candidate);
                hasExistingData = true;
                stateDirectory = candidate;
            }
        }

        if (stateDirectory == null)
            stateDirectory = stateDirectory(directories[0]);

        boolean rebuildNeeded = !hasExistingData || forceRebuild();

        if (truncateBallotMetadata() && !rebuildNeeded)
            logger.warn("{} was set to true, but {} was not and no rebuild is required. Ballot data will not be truncated",
                        TRUNCATE_BALLOT_METADATA.getKey(), FORCE_PAXOS_STATE_REBUILD.getKey());

        if (rebuildNeeded)
        {
            if (stateDirectory.exists())
            {
                PaxosUncommittedTracker.truncate(stateDirectory);
                if (truncateBallotMetadata())
                    PaxosBallotTracker.truncate(stateDirectory);
            }
            else
            {
                stateDirectory.createDirectoriesIfNotExists();
            }
        }

        PaxosUncommittedTracker uncommitted = PaxosUncommittedTracker.load(stateDirectory);
        PaxosBallotTracker ballots = PaxosBallotTracker.load(stateDirectory);

        if (!rebuildNeeded)
            uncommitted.start();

        return new PaxosStateTracker(uncommitted, ballots, rebuildNeeded);
    }

    public static PaxosStateTracker create(Directories.DataDirectories dataDirectories) throws IOException
    {
        return create(dataDirectories.getAllDirectories().stream().map(d -> d.location).toArray(File[]::new));
    }

    private void rebuildUncommittedData() throws IOException
    {
        logger.info("Beginning uncommitted paxos data rebuild. Set -D{}=true and restart to skip", SKIP_PAXOS_STATE_REBUILD.getKey());

        String queryStr = "SELECT * FROM " + SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PAXOS;
        SelectStatement stmt = (SelectStatement) QueryProcessor.parseStatement(queryStr).prepare(ClientState.forInternalCalls());
        ReadQuery query = stmt.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        try (ReadExecutionController controller = query.executionController();
             PartitionIterator partitions = query.executeInternal(controller);
             PaxosKeyStateRowsIterator rows = new PaxosKeyStateRowsIterator(partitions))
        {
            uncommitted.rebuild(rows);
        }
    }

    class PaxosKeyStateRowsIterator implements CloseableIterator<PaxosKeyState>
    {
        // note: this is not closed by this iterator
        final PartitionIterator partitions;

        RowIterator partition = null;
        PaxosKeyState next = null;

        PaxosKeyStateRowsIterator(PartitionIterator partitions)
        {
            this.partitions = partitions;
        }

        @Override
        public boolean hasNext()
        {
            if (next != null)
                return true;

            while (true)
            {
                if (partition != null && partition.hasNext())
                {
                    PaxosKeyState commitState = PaxosRows.getCommitState(partition.partitionKey(), partition.next(), null);
                    if (commitState == null)
                        continue;
                    ballots.updateHighBound(commitState.ballot);
                    if (!commitState.committed)
                    {
                        next = commitState;
                        return true;
                    }
                }
                else
                {
                    if (partition != null)
                    {
                        partition.close();
                        partition = null;
                    }

                    if (!partitions.hasNext())
                        return false;

                    partition = partitions.next();
                }
            }
        }

        @Override
        public PaxosKeyState next()
        {
            if (next == null && !hasNext())
                throw new NoSuchElementException();
            PaxosKeyState next = this.next;
            this.next = null;
            return next;
        }

        @Override
        public void close()
        {
            if (partition != null)
            {
                partition.close();
                partition = null;
            }
        }
    }

    private void updateLowBoundFromRepairHistory() throws IOException
    {
        String queryStr = "SELECT * FROM " + SYSTEM_KEYSPACE_NAME + '.' + PAXOS_REPAIR_HISTORY;
        SelectStatement stmt = (SelectStatement) QueryProcessor.parseStatement(queryStr).prepare(ClientState.forInternalCalls());
        ReadQuery query = stmt.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());

        Ballot lowBound = null;
        ListType<ByteBuffer> listType = ListType.getInstance(BytesType.instance, false);
        ColumnMetadata pointsColumn = ColumnMetadata.regularColumn(SYSTEM_KEYSPACE_NAME, PAXOS_REPAIR_HISTORY, "points", listType);
        try (ReadExecutionController controller = query.executionController(); PartitionIterator partitions = query.executeInternal(controller))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    String keyspaceName = UTF8Type.instance.compose(partition.partitionKey().getKey());
                    if (Schema.instance.getKeyspaceMetadata(keyspaceName) == null)
                        continue;

                    Keyspace.open(keyspaceName);
                    while (partition.hasNext())
                    {
                        Row row = partition.next();
                        Clustering clustering = row.clustering();
                        String tableName = UTF8Type.instance.compose(clustering.get(0), clustering.accessor());
                        if (Schema.instance.getTableMetadata(keyspaceName, tableName) == null)
                            continue;

                        Cell pointsCell = row.getCell(pointsColumn);
                        List<ByteBuffer> points = listType.compose(pointsCell.value(), pointsCell.accessor());
                        PaxosRepairHistory history = PaxosRepairHistory.fromTupleBufferList(points);
                        lowBound = Commit.latest(lowBound, history.maxLowBound());
                    }
                }
            }
        }
        ballots.updateLowBound(lowBound);
    }

    public void maybeRebuild() throws IOException
    {
        if (!rebuildNeeded)
            return;

        if (truncateBallotMetadata())
        {
            logger.info("Truncating {}.{}", SYSTEM_KEYSPACE_NAME, PAXOS_REPAIR_HISTORY);
            Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(PAXOS_REPAIR_HISTORY).truncateBlocking();
        }

        if (!skipRebuild())
        {
            rebuildUncommittedData();

            if (!truncateBallotMetadata()) // no point doing this if we just truncated the repair history table
                updateLowBoundFromRepairHistory();
            logger.info("Uncommitted paxos data rebuild completed");
        }
        uncommitted.start();
        ballots.flush();   // explicitly flush since a missing ballot file on startup indicates a rebuild is needed
        rebuildNeeded = false;
    }

    public PaxosUncommittedTracker uncommitted()
    {
        return uncommitted;
    }

    public PaxosBallotTracker ballots()
    {
        return ballots;
    }
}
