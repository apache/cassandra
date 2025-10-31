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

package org.apache.cassandra.service.replication.migration;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.virtual.VirtualMutation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkState;

/**
 * Routes read and write requests based on schema and migration state.
 *
 * During migration in either direction, reads are untracked and writes are tracked
 */
public class MigrationRouter
{
    public static boolean shouldUseTracked(SinglePartitionReadCommand command)
    {
        // System keyspaces never use tracked replication
        if (SchemaConstants.isSystemKeyspace(command.metadata().keyspace))
            return false;

        ClusterMetadata metadata = ClusterMetadata.current();

        KeyspaceMigrationInfo migrationInfo = metadata.mutationTrackingMigrationState.getKeyspaceInfo(command.metadata().keyspace);

        if (migrationInfo == null)
            return command.metadata().replicationType().isTracked();

        Token token = command.partitionKey().getToken();
        boolean isTracked = command.metadata().replicationType().isTracked();

        return migrationInfo.shouldUseTrackedForReads(isTracked, command.metadata().id(), token);
    }

    /**
     * Wrapper for a range read command paired with its routing decision.
     */
    public static class RangeReadWithReplication
    {
        public final PartitionRangeReadCommand read;
        public final boolean useTracked;

        public RangeReadWithReplication(PartitionRangeReadCommand read, boolean useTracked)
        {
            this.read = read;
            this.useTracked = useTracked;
        }
    }

    /**
     * Helper to create and add a range split to the result list.
     */
    private static void addSplit(List<RangeReadWithReplication> result,
                                 PartitionRangeReadCommand command,
                                 AbstractBounds<PartitionPosition> range,
                                 boolean isTracked)
    {
        boolean isFirst = result.isEmpty();
        result.add(new RangeReadWithReplication(command.forSubRange(range, isFirst), isTracked));
    }

    /**
     * Adds a split for the non-pending region before pendingRange, if one exists.
     *
     * @param isTracked the target replication type (TO_TRACKED=true, TO_UNTRACKED=false)
     * @return true if remainder ends before pendingRange (no intersection possible)
     */
    private static boolean addNonPendingGapIfExists(List<RangeReadWithReplication> result,
                                                    PartitionRangeReadCommand command,
                                                    AbstractBounds<PartitionPosition> remainder,
                                                    Range<Token> pendingRange,
                                                    boolean isTracked)
    {
        Token pendingStart = pendingRange.left;
        Token remainderStart = remainder.left.getToken();
        Token remainderEnd = remainder.right.getToken();

        if (remainderStart.compareTo(pendingStart) >= 0)
            return false; // No gap before pending range

        // Check if remainder ends before pending range starts
        if (remainderEnd.compareTo(pendingStart) <= 0)
        {
            // Entire remainder is before this pending range - no intersection
            // Non-pending regions use the new protocol (isTracked)
            addSplit(result, command, remainder, isTracked);
            return true;
        }

        // Add the non-pending gap before pending range
        AbstractBounds<PartitionPosition> gap = remainder.withNewRight(pendingStart.maxKeyBound());

        if (!gap.left.equals(gap.right))
            addSplit(result, command, gap, isTracked);

        return false;
    }

    /**
     * Split a range by pending ranges, creating sub-ranges for each contiguous region.
     * <p>
     * If we're migrating to tracked replication, pending ranges use untracked reads, non-pending uses tracked
     * <p>
     * If we're migrating to untracked replication, pending uses tracked reads, and non-pending uses untracked
     */
    private static List<RangeReadWithReplication> splitRangeByPendingRanges(PartitionRangeReadCommand command,
                                                                            AbstractBounds<PartitionPosition> keyRange,
                                                                            NormalizedRanges<Token> pendingRanges,
                                                                            boolean isTracked)
    {
        Preconditions.checkArgument(!AbstractBounds.strictlyWrapsAround(keyRange.left, keyRange.right));

        List<RangeReadWithReplication> result = new ArrayList<>();
        AbstractBounds<PartitionPosition> remainder = keyRange;

        for (Range<Token> pendingRange : pendingRanges)
        {
            // Add non-pending gap before this pending range (if exists)
            if (addNonPendingGapIfExists(result, command, remainder, pendingRange, isTracked))
            {
                remainder = null;
                break; // No more remainder to process
            }

            // Add intersection with pending range
            Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> split =
            Range.intersectionAndRemainder(remainder, pendingRange);

            // Pending regions use the old protocol (!isTracked)
            if (split.left != null)
                addSplit(result, command, split.left, !isTracked);

            remainder = split.right;
            if (remainder == null)
                break;
        }

        // Add final non-pending remainder
        if (remainder != null)
            addSplit(result, command, remainder, isTracked);

        return result;
    }

    /**
     * Validate that splits are contiguous, cover the entire original range, and alternate protocols.
     */
    private static void validateSplitContiguity(PartitionRangeReadCommand originalCommand,
                                                List<RangeReadWithReplication> splits)
    {
        checkState(!splits.isEmpty(), "Shouldn't have empty result");

        // Validate coverage
        checkState(splits.get(0).read.dataRange().startKey()
                                     .equals(originalCommand.dataRange().startKey()),
                   "Split reads should encompass entire range");
        checkState(splits.get(splits.size() - 1).read.dataRange().stopKey()
                                                     .equals(originalCommand.dataRange().stopKey()),
                   "Split reads should encompass entire range");

        // Validate contiguity and alternating protocols
        if (splits.size() > 1)
        {
            for (int i = 0; i < splits.size() - 1; i++)
            {
                checkState(splits.get(i).read.dataRange().stopKey()
                                             .equals(splits.get(i + 1).read.dataRange().startKey()),
                           "Split reads should all be adjacent");
                checkState(splits.get(i).useTracked != splits.get(i + 1).useTracked,
                           "Split reads should be for different replication protocols");
            }
        }
    }

    /**
     * Split a range read command into sub-ranges based on migration state.
     */
    public static List<RangeReadWithReplication> splitRangeRead(ClusterMetadata metadata,
                                                                PartitionRangeReadCommand command)
    {
        // System keyspaces never use tracked replication
        if (SchemaConstants.isSystemKeyspace(command.metadata().keyspace))
            return ImmutableList.of(new RangeReadWithReplication(command, false));

        KeyspaceMigrationInfo migrationInfo = metadata.mutationTrackingMigrationState
                                              .getKeyspaceInfo(command.metadata().keyspace);

        boolean isTracked = command.metadata().replicationType().isTracked();

        // During migration, reads use untracked replication except for ranges that have
        // completed migration to tracked. Therefore, we only need to split ranges when
        // migrating to tracked replication. For untracked migrations, all reads use untracked.
        if (!isTracked || migrationInfo == null)
            return ImmutableList.of(new RangeReadWithReplication(command, isTracked));

        // Get pending ranges for this table
        NormalizedRanges<Token> tablePendingRanges = migrationInfo.pendingRangesPerTable.get(command.metadata().id());

        // No pending ranges for this table - entire range uses current protocol
        if (tablePendingRanges == null)
            return ImmutableList.of(new RangeReadWithReplication(command, isTracked));

        // split into pending (untracked) and non-pending (tracked) ranges
        List<RangeReadWithReplication> result = splitRangeByPendingRanges(
        command,
        command.dataRange().keyRange(),
        tablePendingRanges,
        isTracked);

        // Validate the splits
        validateSplitContiguity(command, result);

        return result;
    }

    public static boolean shouldUseTrackedForWrites(ClusterMetadata metadata, String keyspace, TableId tableId, Token token)
    {
        if (SchemaConstants.isSystemKeyspace(keyspace))
            return false;

        KeyspaceMigrationInfo migrationInfo = metadata.mutationTrackingMigrationState
                                              .getKeyspaceInfo(keyspace);

        if (migrationInfo == null)
            return metadata.schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked();

        boolean isTracked = metadata.schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked();
        return migrationInfo.shouldUseTrackedForWrites(isTracked, tableId, token);
    }

    public static class RoutedMutations
    {
        public final List<? extends IMutation> trackedMutations;
        public final List<? extends IMutation> untrackedMutations;

        public RoutedMutations(List<? extends IMutation> tracked, List<? extends IMutation> untracked)
        {
            this.trackedMutations = tracked;
            this.untrackedMutations = untracked;
        }
    }

    /**
     * Route a list of mutations, splitting them into tracked and untracked groups.
     */
    @VisibleForTesting
    static RoutedMutations routeMutations(ClusterMetadata cm, List<? extends IMutation> mutations)
    {
        List<IMutation> tracked = new ArrayList<>();
        List<IMutation> untracked = new ArrayList<>();

        for (IMutation mutation : mutations)
        {
            if (mutation instanceof VirtualMutation)
            {
                untracked.add(mutation);
                continue;
            }

            // we need to router system keyspace mutations before CMS is ready
            if (cm == null && !SchemaConstants.isSystemKeyspace(mutation.getKeyspaceName()))
                cm = ClusterMetadata.current();

            {
                ClusterMetadata cm0 = cm;
                IMutation untrackedMutation = mutation.filter(tid -> !shouldUseTrackedForWrites(cm0, mutation.getKeyspaceName(), tid, mutation.key().getToken()));
                if (untrackedMutation != null)
                    untracked.add(untrackedMutation);

                IMutation trackedMutation = mutation.filter(tid -> shouldUseTrackedForWrites(cm0, mutation.getKeyspaceName(), tid, mutation.key().getToken()));
                if (trackedMutation != null)
                    tracked.add(trackedMutation);
            }

        }

        return new RoutedMutations(tracked, untracked);
    }

    public static RoutedMutations routeMutations(List<? extends IMutation> mutations)
    {
        return routeMutations(null, mutations);
    }

    public enum MutationRouting
    {
        TRACKED, UNTRACKED, MIXED
    }

    public static MutationRouting getMutationRouting(ClusterMetadata cm, IMutation mutation)
    {
        // System keyspaces always use untracked replication
        if (SchemaConstants.isSystemKeyspace(mutation.getKeyspaceName()))
            return MutationRouting.UNTRACKED;

        if (cm == null)
            cm = ClusterMetadata.current();
        String keyspace = mutation.getKeyspaceName();
        Token token = mutation.key().getToken();

        MutationRouting result = null;
        for (TableId tableId : mutation.getTableIds())
        {
            MutationRouting routing = shouldUseTrackedForWrites(cm, keyspace, tableId, token)
                                      ? MutationRouting.TRACKED
                                      : MutationRouting.UNTRACKED;

            if (result == null)
                result = routing;
            else if (result != routing)
                return MutationRouting.MIXED;
        }

        return result != null ? result : MutationRouting.UNTRACKED;
    }

    public static MutationRouting getMutationRouting(IMutation mutation)
    {
        return getMutationRouting(null, mutation);
    }


    public static boolean isFullyTracked(IMutation mutation)
    {
        return getMutationRouting(mutation) == MutationRouting.TRACKED;
    }

    private static void validateMutationReplication(IMutation mutation, MutationRouting expected)
    {
        switch (expected)
        {
            case TRACKED:
                if (mutation.id().isNone())
                    throw new IllegalArgumentException();
                break;
            case UNTRACKED:
                if (!mutation.id().isNone())
                    throw new IllegalArgumentException();
                break;
            default:
                throw new IllegalArgumentException();

        }

        MutationRouting actual = getMutationRouting(mutation);
        if (expected != actual)
            throw new CoordinatorBehindException("Mutation replication mismatch: expected " + expected + ", actual " + actual);
    }

    public static void validateTrackedMutation(IMutation mutation)
    {
        validateMutationReplication(mutation, MutationRouting.TRACKED);
    }

    public static void validateUntrackedMutation(IMutation mutation)
    {
        validateMutationReplication(mutation, MutationRouting.UNTRACKED);
    }
}
