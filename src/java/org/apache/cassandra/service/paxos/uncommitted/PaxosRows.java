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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Commit.Accepted;
import org.apache.cassandra.service.paxos.Commit.AcceptedWithTTL;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.service.paxos.Commit.CommittedWithTTL;
import org.apache.cassandra.service.paxos.PaxosPartitionUpdateBlob;
import org.apache.cassandra.service.paxos.PaxosPartitionUpdateBlob.Decoded;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

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
        return ColumnMetadata.regularColumn(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS, name, type, ColumnMetadata.NO_UNIQUE_ID);
    }

    public static Ballot getPromise(Row row)
    {
        return getBallot(row, READ_PROMISE, Ballot.none());
    }

    public static Ballot getWritePromise(Row row)
    {
        return getBallot(row, WRITE_PROMISE, Ballot.none());
    }

    public static Accepted getAccepted(DecoratedKey partitionKey, Row row, long purgeBefore, long overrideTtlSeconds)
    {
        Cell ballotCell = row.getCell(PROPOSAL);
        if (ballotCell == null)
            return null;

        // The column ballot can belong to a different write than the blob when two writes share a
        // timestamp. Every cell of a write carries ballot.unixMicros(), so both ballots have the
        // same microsecond and differ only in the flag digit and the low bits. The purge horizon
        // is coarser than a microsecond, so the column ballot answers the purge check as the
        // blob ballot would. The returned ballot comes from the blob when the blob carries one.
        Ballot columnBallot = ballotCell.accessor().toBallot(ballotCell.value());
        if (columnBallot.uuidTimestamp() < purgeBefore)
            return null;

        Decoded decoded = getUpdate(partitionKey, row, PROPOSAL_UPDATE, PROPOSAL_VERSION);
        Ballot ballot = decoded.ballot != null ? decoded.ballot : columnBallot;
        PartitionUpdate update = decoded.update;
        if (overrideTtlSeconds > 0) return new AcceptedWithTTL(ballot, update, TimeUnit.MICROSECONDS.toSeconds(ballotCell.timestamp()) + overrideTtlSeconds);
        else if (ballotCell.isExpiring()) return new AcceptedWithTTL(ballot, update, ballotCell.localDeletionTime());
        else return new Accepted(ballot, update);
    }

    public static Committed getCommitted(TableMetadata metadata, DecoratedKey partitionKey, Row row, long purgeBefore, long overrideTtlSeconds)
    {
        Cell ballotCell = row.getCell(COMMIT);
        if (ballotCell == null)
            return Committed.none(partitionKey, metadata);

        // See getAccepted for constraints on using committed ballot
        Ballot columnBallot = ballotCell.accessor().toBallot(ballotCell.value());
        if (columnBallot.uuidTimestamp() < purgeBefore)
            return Committed.none(partitionKey, metadata);

        Decoded decoded = getUpdate(partitionKey, row, COMMIT_UPDATE, COMMIT_VERSION);
        Ballot ballot = decoded.ballot != null ? decoded.ballot : columnBallot;
        PartitionUpdate update = decoded.update;
        if (overrideTtlSeconds > 0) return new CommittedWithTTL(ballot, update, TimeUnit.MICROSECONDS.toSeconds(ballotCell.timestamp()) + overrideTtlSeconds);
        else if (ballotCell.isExpiring()) return new CommittedWithTTL(ballot, update, ballotCell.localDeletionTime());
        else return new Committed(ballot, update);
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
        if (cell == null || cell.isTombstone())
            return ifNull;
        return Int32Type.instance.compose(cell.value(), cell.accessor());
    }

    private static Decoded getUpdate(DecoratedKey partitionKey, Row row, ColumnMetadata blobColumn, ColumnMetadata versionColumn)
    {
        Cell cell = row.getCell(blobColumn);
        if (cell == null)
            throw new IllegalStateException();

        return PaxosPartitionUpdateBlob.decode(cell.buffer(), getInt(row, versionColumn, PaxosPartitionUpdateBlob.VERSION_ABSENT),
                                               partitionKey.getKey(), getTableUuid(row));
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
        Cell proposal = row.getCell(PROPOSAL_UPDATE);
        if (proposal == null)
            return true;

        return PaxosPartitionUpdateBlob.updateIsEmpty(proposal.buffer(), getInt(row, PROPOSAL_VERSION, PaxosPartitionUpdateBlob.VERSION_ABSENT),
                                                      key.getKey(), getTableUuid(row));
    }

    private static long getTimestamp(Row row, ColumnMetadata cmeta)
    {
        Cell cell = row.getCell(cmeta);
        if (cell == null || cell.valueSize() == 0)
            return Long.MIN_VALUE;
        return cell.timestamp();
    }

    /**
     * Classifies a row as committed or in progress from its ballot columns. It takes the empty
     * check from the proposal blob and pairs that answer with the {@code proposal_ballot} column.
     * Two proposals written on one microsecond can leave the column and the blob from different
     * writes.
     */
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
            // An undecodable proposal keeps the key in progress. Paxos repair retries an in progress
            // key. The failure stays on this key and the scan continues with the others.
            boolean empty;
            try
            {
                empty = proposalIsEmpty(row, key);
            }
            catch (MarshalException e)
            {
                empty = false;
            }
            if (empty)
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

    /**
     * Returns the ballot of the newest ballot column, chosen by cell timestamp. The caller uses
     * it as a time horizon and never pairs it with an update, so a tie between two ballots on
     * one microsecond does not affect the result.
     */
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

    /**
     * Returns true only if every ballot column is present and holds a ballot at or after
     * {@code ballot}. A missing ballot column makes the result false. The comparison uses the
     * ballot timestamp, so two ballots on one microsecond compare equal.
     */
    public static boolean hasBallotBeforeOrEqualTo(Row row, Ballot ballot)
    {
        return !Commit.isAfter(ballot, getBallot(row, WRITE_PROMISE))
            && !Commit.isAfter(ballot, getBallot(row, READ_PROMISE))
            && !Commit.isAfter(ballot, getBallot(row, PROPOSAL))
            && !Commit.isAfter(ballot, getBallot(row, COMMIT));
    }
}
