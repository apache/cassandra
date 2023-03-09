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
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Commit.Accepted;
import org.apache.cassandra.service.paxos.Commit.AcceptedWithTTL;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.service.paxos.Commit.CommittedWithTTL;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.db.partitions.PartitionUpdate.PartitionUpdateSerializer.*;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.Commit.latest;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class PaxosRows
{
    private static final ColumnMetadata WRITE_PROMISE = paxosColumn("in_progress_ballot", TimeUUIDType.instance);
    private static final ColumnMetadata READ_PROMISE = paxosColumn("in_progress_read_ballot", TimeUUIDType.instance);
    private static final ColumnMetadata PROPOSAL = paxosColumn("proposal_ballot", TimeUUIDType.instance);
    private static final ColumnMetadata PROPOSAL_UPDATE = paxosColumn("proposal", BytesType.instance);
    private static final ColumnMetadata PROPOSAL_VERSION = paxosColumn("proposal_version", Int32Type.instance);
    private static final ColumnMetadata COMMIT = paxosColumn("most_recent_commit_at", TimeUUIDType.instance);
    private static final ColumnMetadata COMMIT_UPDATE = paxosColumn("most_recent_commit", BytesType.instance);
    private static final ColumnMetadata COMMIT_VERSION = paxosColumn("most_recent_commit_version", Int32Type.instance);

    private PaxosRows() {}

    private static ColumnMetadata paxosColumn(String name, AbstractType<?> type)
    {
        return ColumnMetadata.regularColumn(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS, name, type);
    }

    public static Ballot getPromise(Row row)
    {
        return getBallot(row, READ_PROMISE, Ballot.none());
    }

    public static Ballot getWritePromise(Row row)
    {
        return getBallot(row, WRITE_PROMISE, Ballot.none());
    }

    public static Accepted getAccepted(Row row)
    {
        Cell ballotCell = row.getCell(PROPOSAL);
        if (ballotCell == null)
            return null;

        Ballot ballot = ballotCell.accessor().toBallot(ballotCell.value());
        int version = getInt(row, PROPOSAL_VERSION, MessagingService.VERSION_40);
        PartitionUpdate update = getUpdate(row, PROPOSAL_UPDATE, version);
        return ballotCell.isExpiring()
               ? new AcceptedWithTTL(ballot, update, ballotCell.localDeletionTime())
               : new Accepted(ballot, update);
    }

    public static Committed getCommitted(TableMetadata metadata, DecoratedKey partitionKey, Row row)
    {
        Cell ballotCell = row.getCell(COMMIT);
        if (ballotCell == null)
            return Committed.none(partitionKey, metadata);

        Ballot ballot = ballotCell.accessor().toBallot(ballotCell.value());
        int version = getInt(row, COMMIT_VERSION, MessagingService.VERSION_40);
        PartitionUpdate update = getUpdate(row, COMMIT_UPDATE, version);
        return ballotCell.isExpiring()
               ? new CommittedWithTTL(ballot, update, ballotCell.localDeletionTime())
               : new Committed(ballot, update);
    }

    public static TableId getTableId(Row row)
    {
        return TableId.fromUUID(UUIDType.instance.compose(row.clustering().get(0), (ValueAccessor)row.clustering().accessor()));
    }

    public static UUID getTableUuid(Row row)
    {
        return UUIDType.instance.compose(row.clustering().get(0), (ValueAccessor)row.clustering().accessor());
    }

    private static int getInt(Row row, ColumnMetadata cmeta, @SuppressWarnings("SameParameterValue") int ifNull)
    {
        Cell cell = row.getCell(cmeta);
        if (cell == null)
            return ifNull;
        return Int32Type.instance.compose(cell.value(), cell.accessor());
    }

    private static PartitionUpdate getUpdate(Row row, ColumnMetadata cmeta, int version)
    {
        Cell cell = row.getCell(cmeta);
        if (cell == null)
            throw new IllegalStateException();

        return PartitionUpdate.fromBytes(cell.buffer(), version);
    }

    private static Ballot getBallot(Row row, ColumnMetadata cmeta)
    {
        return getBallot(row, cmeta, null);
    }

    private static Ballot getBallot(Row row, ColumnMetadata cmeta, Ballot ifNull)
    {
        Cell cell = row.getCell(cmeta);
        if (cell == null)
            return ifNull;
        return cell.accessor().toBallot(cell.value());
    }

    private static boolean proposalIsEmpty(Row row, DecoratedKey key)
    {
        try
        {
            Cell proposalVersionCell = row.getCell(PROPOSAL_VERSION);
            if (proposalVersionCell == null)
                return true;
            Integer proposalVersion = Int32Type.instance.compose(proposalVersionCell.value(), proposalVersionCell.accessor());
            if (proposalVersion == null)
                return true;

            Cell proposal = row.getCell(PROPOSAL_UPDATE);
            if (proposal == null)
                return true;

            ByteBuffer proposalValue = proposal.buffer();
            if (!proposalValue.hasRemaining())
                return true;

            return isEmpty(proposalValue, DeserializationHelper.Flag.LOCAL, key);
        }
        catch (IOException e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            throw new RuntimeException(e);
        }
    }

    private static long getTimestamp(Row row, ColumnMetadata cmeta)
    {
        Cell cell = row.getCell(cmeta);
        if (cell == null || cell.valueSize() == 0)
            return Long.MIN_VALUE;
        return cell.timestamp();
    }

    static PaxosKeyState getCommitState(DecoratedKey key, Row row, TableId targetTableId)
    {
        if (row == null)
            return null;

        UUID tableUuid = getTableUuid(row);
        if (targetTableId != null && !targetTableId.asUUID().equals(tableUuid))
            return null;

        Ballot promise = latest(getBallot(row, WRITE_PROMISE), getBallot(row, READ_PROMISE));
        Ballot proposal = getBallot(row, PROPOSAL);
        Ballot commit = getBallot(row, COMMIT);

        Ballot inProgress = null;
        Ballot committed = null;
        if (isAfter(promise, proposal))
        {
            if (isAfter(promise, commit))
                inProgress = promise;
            else
                committed = commit;
        }
        else if (isAfter(proposal, commit))
        {
            if (proposalIsEmpty(row, key))
                committed = proposal;
            else
                inProgress = proposal;
        }
        else
        {
            committed = commit;
        }

        TableId tableId = TableId.fromUUID(tableUuid);
        return inProgress != null ?
               new PaxosKeyState(tableId, key, inProgress, false) :
               new PaxosKeyState(tableId, key, committed, true);
    }

    private static class PaxosMemtableToKeyStateIterator extends AbstractIterator<PaxosKeyState> implements CloseableIterator<PaxosKeyState>
    {
        private final UnfilteredPartitionIterator partitions;
        private UnfilteredRowIterator partition;
        private final @Nullable TableId filterByTableId; // if unset, return records for all tables

        private PaxosMemtableToKeyStateIterator(UnfilteredPartitionIterator partitions, TableId filterByTableId)
        {
            this.partitions = partitions;
            this.filterByTableId = filterByTableId;
        }

        protected PaxosKeyState computeNext()
        {
            while (true)
            {
                if (partition != null && partition.hasNext())
                {
                    PaxosKeyState commitState = PaxosRows.getCommitState(partition.partitionKey(),
                                                                         (Row) partition.next(),
                                                                         filterByTableId);
                    if (commitState == null)
                        continue;

                    return commitState;
                }
                else if (partition != null)
                {
                    partition.close();
                    partition = null;
                }

                if (partitions.hasNext())
                {
                    partition = partitions.next();
                }
                else
                {
                    partitions.close();
                    return endOfData();
                }
            }
        }

        public void close()
        {
            if (partition != null)
                partition.close();
            partitions.close();
        }
    }

    static CloseableIterator<PaxosKeyState> toIterator(UnfilteredPartitionIterator partitions, TableId filterBytableId, boolean materializeLazily)
    {
        CloseableIterator<PaxosKeyState> iter = new PaxosMemtableToKeyStateIterator(partitions, filterBytableId);
        if (materializeLazily)
            return iter;

        try
        {
            // eagerly materialize key states for repairs so we're not referencing memtables for the entire repair
            return CloseableIterator.wrap(Lists.newArrayList(iter).iterator());
        }
        finally
        {
            iter.close();
        }
    }

    public static Ballot getHighBallot(Row row, Ballot current)
    {
        long maxUnixMicros = current != null ? current.unixMicros() : Long.MIN_VALUE;
        ColumnMetadata maxCol = null;

        long inProgressRead = getTimestamp(row, READ_PROMISE);
        if (inProgressRead > maxUnixMicros)
        {
            maxUnixMicros = inProgressRead;
            maxCol = READ_PROMISE;
        }

        long inProgressWrite = getTimestamp(row, WRITE_PROMISE);
        if (inProgressWrite > maxUnixMicros)
        {
            maxUnixMicros = inProgressWrite;
            maxCol = WRITE_PROMISE;
        }

        long proposal = getTimestamp(row, PROPOSAL);
        if (proposal > maxUnixMicros)
        {
            maxUnixMicros = proposal;
            maxCol = PROPOSAL;
        }

        long commit = getTimestamp(row, COMMIT);
        if (commit > maxUnixMicros)
            maxCol = COMMIT;

        return maxCol == null ? current : getBallot(row, maxCol);
    }

    public static boolean hasBallotBeforeOrEqualTo(Row row, Ballot ballot)
    {
        return !Commit.isAfter(ballot, getBallot(row, WRITE_PROMISE))
            && !Commit.isAfter(ballot, getBallot(row, READ_PROMISE))
            && !Commit.isAfter(ballot, getBallot(row, PROPOSAL))
            && !Commit.isAfter(ballot, getBallot(row, COMMIT));
    }
}
