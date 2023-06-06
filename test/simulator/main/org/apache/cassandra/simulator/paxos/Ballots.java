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

package org.apache.cassandra.simulator.paxos;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.simulator.systems.NonInterceptible;
import org.apache.cassandra.simulator.systems.NonInterceptible.Permit;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.Long.max;
import static java.util.Arrays.stream;
import static org.apache.cassandra.db.SystemKeyspace.loadPaxosState;
import static org.apache.cassandra.service.paxos.Commit.latest;
import static org.apache.cassandra.service.paxos.PaxosState.unsafeGetIfPresent;
import static org.apache.cassandra.simulator.systems.NonInterceptible.Permit.OPTIONAL;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

public class Ballots
{
    private static final ColumnMetadata PROMISE = paxosUUIDColumn("in_progress_ballot");
    private static final ColumnMetadata PROPOSAL = paxosUUIDColumn("proposal_ballot");
    private static final ColumnMetadata COMMIT = paxosUUIDColumn("most_recent_commit_at");

    @Shared(scope = SIMULATION)
    public static class LatestBallots
    {
        public final long promise;
        public final long accept; // the ballot actually accepted
        public final long acceptOf; // the original ballot (i.e. if a reproposal accept != acceptOf)
        public final long commit;
        public final long persisted;

        public LatestBallots(long promise, long accept, long acceptOf, long commit, long persisted)
        {
            this.promise = promise;
            this.accept = accept;
            this.acceptOf = acceptOf;
            this.commit = commit;
            this.persisted = persisted;
        }

        public long any()
        {
            return max(max(max(promise, accept), commit), persisted);
        }

        public long permanent()
        {
            return max(commit, persisted);
        }

        public String toString()
        {
            return "[" + promise + ',' + accept + ',' + commit + ',' + persisted + ']';
        }
    }

    public static LatestBallots read(Permit permit, DecoratedKey key, TableMetadata metadata, long nowInSec, boolean includeEmptyProposals)
    {
        return NonInterceptible.apply(permit, () -> {
            PaxosState.Snapshot state = unsafeGetIfPresent(key, metadata);
            PaxosState.Snapshot persisted = loadPaxosState(key, metadata, nowInSec);
            TimeUUID promised = latest(persisted.promised, state == null ? null : state.promised);
            Commit.Accepted accepted = latest(persisted.accepted, state == null ? null : state.accepted);
            Commit.Committed committed = latest(persisted.committed, state == null ? null : state.committed);
            long baseTable = latestBallotFromBaseTable(key, metadata);
            return new LatestBallots(
                promised.unixMicros(),
                accepted == null || accepted.update.isEmpty() ? 0L : accepted.ballot.unixMicros(),
                accepted == null || accepted.update.isEmpty() ? 0L : accepted.update.stats().minTimestamp,
                latestBallot(committed.update.iterator()),
                baseTable
            );
        });
    }

    static LatestBallots[][] read(Permit permit, Cluster cluster, String keyspace, String table, int[] primaryKeys, int[][] replicasForKeys, boolean includeEmptyProposals)
    {
        return NonInterceptible.apply(permit, () -> {
            LatestBallots[][] result = new LatestBallots[primaryKeys.length][];
            for (int i = 0 ; i < primaryKeys.length ; ++i)
            {
                int primaryKey = primaryKeys[i];
                result[i] = stream(replicasForKeys[i])
                            .mapToObj(cluster::get)
                            .map(node -> node.unsafeApplyOnThisThread((p, ks, tbl, pk, ie) -> {
                                TableMetadata metadata = Keyspace.open(ks).getColumnFamilyStore(tbl).metadata.get();
                                DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(pk));
                                return read(p, key, metadata, FBUtilities.nowInSeconds(), ie);
                            }, permit, keyspace, table, primaryKey, includeEmptyProposals))
                            .toArray(LatestBallots[]::new);
            }
            return result;
        });
    }

    public static String paxosDebugInfo(DecoratedKey key, TableMetadata metadata, long nowInSec)
    {
        return NonInterceptible.apply(OPTIONAL, () -> {
            PaxosState.Snapshot state = unsafeGetIfPresent(key, metadata);
            PaxosState.Snapshot persisted = loadPaxosState(key, metadata, nowInSec);
            long[] memtable = latestBallotsFromPaxosMemtable(key, metadata);
            PaxosState.Snapshot cache = state == null ? persisted : state;
            long baseTable = latestBallotFromBaseTable(key, metadata);
            long baseMemtable = latestBallotFromBaseMemtable(key, metadata);
            return debugBallot(cache.promised, memtable[0], persisted.promised) + ", "
                   + debugBallot(cache.accepted, memtable[1], persisted.accepted) + ", "
                   + debugBallot(cache.committed, memtable[2], persisted.committed) + ", "
                   + debugBallot(baseMemtable, 0L, baseTable);
        });
    }

    private static ColumnMetadata paxosUUIDColumn(String name)
    {
        return ColumnMetadata.regularColumn(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS, name, TimeUUIDType.instance);
    }

    /**
     * Load the current paxos state for the table and key
     */
    private static long[] latestBallotsFromPaxosMemtable(DecoratedKey key, TableMetadata metadata)
    {
        ColumnFamilyStore paxos = Keyspace.open("system").getColumnFamilyStore("paxos");
        long[] result = new long[3];
        List<Memtable> memtables = ImmutableList.copyOf(paxos.getTracker().getView().getAllMemtables());
        for (Memtable memtable : memtables)
        {
            Row row = getRow(key, metadata, paxos, memtable);
            if (row == null)
                continue;

            Cell promise = row.getCell(PROMISE);
            if (promise != null && promise.value() != null)
                result[0] = promise.timestamp();
            Cell proposal = row.getCell(PROPOSAL);
            if (proposal != null && proposal.value() != null)
                result[1] = proposal.timestamp();
            Cell commit = row.getCell(COMMIT);
            if (commit != null && commit.value() != null)
                result[2] = commit.timestamp();
        }
        return result;
    }

    private static Row getRow(DecoratedKey key, TableMetadata metadata, ColumnFamilyStore paxos, Memtable memtable)
    {
        final ClusteringComparator comparator = paxos.metadata.get().comparator;
        UnfilteredRowIterator iter = memtable.rowIterator(key, Slices.with(comparator, Slice.make(comparator.make(metadata.id))), ColumnFilter.NONE, false, SSTableReadsListener.NOOP_LISTENER);
        if (iter == null || !iter.hasNext())
            return null;
        return (Row) iter.next();
    }

    public static long latestBallotFromBaseTable(DecoratedKey key, TableMetadata metadata)
    {
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(metadata, 0, key, Slice.ALL);
        try (ReadExecutionController controller = cmd.executionController(); UnfilteredPartitionIterator partitions = cmd.executeLocally(controller))
        {
            if (!partitions.hasNext())
                return 0L;

            try (UnfilteredRowIterator rows = partitions.next())
            {
                return latestBallot(rows);
            }
        }
    }

    private static long latestBallotFromBaseMemtable(DecoratedKey key, TableMetadata metadata)
    {
        ColumnFamilyStore table = Keyspace.openAndGetStore(metadata);
        long timestamp = 0;
        List<Memtable> memtables = ImmutableList.copyOf(table.getTracker().getView().getAllMemtables());
        for (Memtable memtable : memtables)
        {
            try (UnfilteredRowIterator partition = memtable.rowIterator(key))
            {
                if (partition == null)
                    continue;

                timestamp = max(timestamp, latestBallot(partition));
            }
        }
        return timestamp;
    }

    private static long latestBallot(Iterator<? extends Unfiltered> partition)
    {
        long timestamp = 0L;
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (!unfiltered.isRow())
                continue;
            timestamp = ((Row) unfiltered).accumulate((cd, v) -> max(v, cd.maxTimestamp()), timestamp);
        }
        return timestamp;
    }

    private static String debugBallot(Commit cache, long memtable, Commit persisted)
    {
        return debugBallot(cache == null ? null : cache.ballot, memtable, persisted == null ? null : persisted.ballot);
    }

    private static String debugBallot(TimeUUID cache, long memtable, TimeUUID persisted)
    {
        return debugBallot(timestamp(cache), memtable, timestamp(persisted));
    }

    private static String debugBallot(long cache, long memtable, long persisted)
    {
        return debugBallotVsMemtable(cache, memtable)
               + (cache == persisted ? "" : '(' + debugBallotVsMemtable(persisted, memtable) + ')');
    }

    private static String debugBallotVsMemtable(long value, long memtable)
    {
        return value + (memtable == value && memtable != 0 ? "*" : "");
    }

    private static long timestamp(TimeUUID a)
    {
        return a == null ? 0L : a.unixMicros();
    }
}
