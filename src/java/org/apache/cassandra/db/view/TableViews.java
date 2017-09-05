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
package org.apache.cassandra.db.view;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;


/**
 * Groups all the views for a given table.
 */
public class TableViews extends AbstractCollection<View>
{
    private final CFMetaData baseTableMetadata;

    // We need this to be thread-safe, but the number of times this is changed (when a view is created in the keyspace)
    // is massively exceeded by the number of times it's read (for every mutation on the keyspace), so a copy-on-write
    // list is the best option.
    private final List<View> views = new CopyOnWriteArrayList();

    public TableViews(CFMetaData baseTableMetadata)
    {
        this.baseTableMetadata = baseTableMetadata;
    }

    public int size()
    {
        return views.size();
    }

    public Iterator<View> iterator()
    {
        return views.iterator();
    }

    public boolean contains(String viewName)
    {
        return Iterables.any(views, view -> view.name.equals(viewName));
    }

    public boolean add(View view)
    {
        // We should have validated that there is no existing view with this name at this point
        assert !contains(view.name);
        return views.add(view);
    }

    public Iterable<ColumnFamilyStore> allViewsCfs()
    {
        Keyspace keyspace = Keyspace.open(baseTableMetadata.ksName);
        return Iterables.transform(views, view -> keyspace.getColumnFamilyStore(view.getDefinition().viewName));
    }

    public void forceBlockingFlush()
    {
        for (ColumnFamilyStore viewCfs : allViewsCfs())
            viewCfs.forceBlockingFlush();
    }

    public void dumpMemtables()
    {
        for (ColumnFamilyStore viewCfs : allViewsCfs())
            viewCfs.dumpMemtable();
    }

    public void truncateBlocking(CommitLogPosition replayAfter, long truncatedAt)
    {
        for (ColumnFamilyStore viewCfs : allViewsCfs())
        {
            viewCfs.discardSSTables(truncatedAt);
            SystemKeyspace.saveTruncationRecord(viewCfs, truncatedAt, replayAfter);
        }
    }

    public void removeByName(String viewName)
    {
        views.removeIf(v -> v.name.equals(viewName));
    }

    /**
     * Calculates and pushes updates to the views replicas. The replicas are determined by
     * {@link ViewUtils#getViewNaturalEndpoint(String, Token, Token)}.
     *
     * @param update an update on the base table represented by this object.
     * @param writeCommitLog whether we should write the commit log for the view updates.
     * @param baseComplete time from epoch in ms that the local base mutation was (or will be) completed
     */
    public void pushViewReplicaUpdates(PartitionUpdate update, boolean writeCommitLog, AtomicLong baseComplete)
    {
        assert update.metadata().cfId.equals(baseTableMetadata.cfId);

        Collection<View> views = updatedViews(update);
        if (views.isEmpty())
            return;

        // Read modified rows
        int nowInSec = FBUtilities.nowInSeconds();
        long queryStartNanoTime = System.nanoTime();
        SinglePartitionReadCommand command = readExistingRowsCommand(update, views, nowInSec);
        if (command == null)
            return;

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(update.metadata());
        long start = System.nanoTime();
        Collection<Mutation> mutations;
        try (ReadExecutionController orderGroup = command.executionController();
             UnfilteredRowIterator existings = UnfilteredPartitionIterators.getOnlyElement(command.executeLocally(orderGroup), command);
             UnfilteredRowIterator updates = update.unfilteredIterator())
        {
            mutations = Iterators.getOnlyElement(generateViewUpdates(views, updates, existings, nowInSec, false));
        }
        Keyspace.openAndGetStore(update.metadata()).metric.viewReadTime.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        if (!mutations.isEmpty())
            StorageProxy.mutateMV(update.partitionKey().getKey(), mutations, writeCommitLog, baseComplete, queryStartNanoTime);
    }


    /**
     * Given some updates on the base table of this object and the existing values for the rows affected by that update, generates the
     * mutation to be applied to the provided views.
     *
     * @param views the views potentially affected by {@code updates}.
     * @param updates the base table updates being applied.
     * @param existings the existing values for the rows affected by {@code updates}. This is used to decide if a view is
     * obsoleted by the update and should be removed, gather the values for columns that may not be part of the update if
     * a new view entry needs to be created, and compute the minimal updates to be applied if the view entry isn't changed
     * but has simply some updated values. This will be empty for view building as we want to assume anything we'll pass
     * to {@code updates} is new.
     * @param nowInSec the current time in seconds.
     * @param separateUpdates, if false, mutation is per partition.
     * @return the mutations to apply to the {@code views}. This can be empty.
     */
    public Iterator<Collection<Mutation>> generateViewUpdates(Collection<View> views,
                                                              UnfilteredRowIterator updates,
                                                              UnfilteredRowIterator existings,
                                                              int nowInSec,
                                                              boolean separateUpdates)
    {
        assert updates.metadata().cfId.equals(baseTableMetadata.cfId);

        List<ViewUpdateGenerator> generators = new ArrayList<>(views.size());
        for (View view : views)
            generators.add(new ViewUpdateGenerator(view, updates.partitionKey(), nowInSec));

        DeletionTracker existingsDeletion = new DeletionTracker(existings.partitionLevelDeletion());
        DeletionTracker updatesDeletion = new DeletionTracker(updates.partitionLevelDeletion());

        /*
         * We iterate through the updates and the existing rows in parallel. This allows us to know the consequence
         * on the view of each update.
         */
        PeekingIterator<Unfiltered> existingsIter = Iterators.peekingIterator(existings);
        PeekingIterator<Unfiltered> updatesIter = Iterators.peekingIterator(updates);

        while (existingsIter.hasNext() && updatesIter.hasNext())
        {
            Unfiltered existing = existingsIter.peek();
            Unfiltered update = updatesIter.peek();

            Row existingRow;
            Row updateRow;
            int cmp = baseTableMetadata.comparator.compare(update, existing);
            if (cmp < 0)
            {
                // We have an update where there was nothing before
                if (update.isRangeTombstoneMarker())
                {
                    updatesDeletion.update(updatesIter.next());
                    continue;
                }

                updateRow = ((Row)updatesIter.next()).withRowDeletion(updatesDeletion.currentDeletion());
                existingRow = emptyRow(updateRow.clustering(), existingsDeletion.currentDeletion());
            }
            else if (cmp > 0)
            {
                // We have something existing but no update (which will happen either because it's a range tombstone marker in
                // existing, or because we've fetched the existing row due to some partition/range deletion in the updates)
                if (existing.isRangeTombstoneMarker())
                {
                    existingsDeletion.update(existingsIter.next());
                    continue;
                }

                existingRow = ((Row)existingsIter.next()).withRowDeletion(existingsDeletion.currentDeletion());
                updateRow = emptyRow(existingRow.clustering(), updatesDeletion.currentDeletion());

                // The way we build the read command used for existing rows, we should always have updatesDeletion.currentDeletion()
                // that is not live, since we wouldn't have read the existing row otherwise. And we could assert that, but if we ever
                // change the read method so that it can slightly over-read in some case, that would be an easily avoiding bug lurking,
                // so we just handle the case.
                if (updateRow == null)
                    continue;
            }
            else
            {
                // We're updating a row that had pre-existing data
                if (update.isRangeTombstoneMarker())
                {
                    assert existing.isRangeTombstoneMarker();
                    updatesDeletion.update(updatesIter.next());
                    existingsDeletion.update(existingsIter.next());
                    continue;
                }

                assert !existing.isRangeTombstoneMarker();
                existingRow = ((Row)existingsIter.next()).withRowDeletion(existingsDeletion.currentDeletion());
                updateRow = ((Row)updatesIter.next()).withRowDeletion(updatesDeletion.currentDeletion());
            }

            addToViewUpdateGenerators(existingRow, updateRow, generators, nowInSec);
        }

        // We only care about more existing rows if the update deletion isn't live, i.e. if we had a partition deletion
        if (!updatesDeletion.currentDeletion().isLive())
        {
            while (existingsIter.hasNext())
            {
                Unfiltered existing = existingsIter.next();
                // If it's a range tombstone, we don't care, we're only looking for existing entry that gets deleted by
                // the new partition deletion
                if (existing.isRangeTombstoneMarker())
                    continue;

                Row existingRow = (Row)existing;
                addToViewUpdateGenerators(existingRow, emptyRow(existingRow.clustering(), updatesDeletion.currentDeletion()), generators, nowInSec);
            }
        }

        if (separateUpdates)
        {
            final Collection<Mutation> firstBuild = buildMutations(baseTableMetadata, generators);

            return new Iterator<Collection<Mutation>>()
            {
                // If the previous values are already empty, this update must be either empty or exclusively appending.
                // In the case we are exclusively appending, we need to drop the build that was passed in and try to build a
                // new first update instead.
                // If there are no other updates, next will be null and the iterator will be empty.
                Collection<Mutation> next = firstBuild.isEmpty()
                                            ? buildNext()
                                            : firstBuild;

                private Collection<Mutation> buildNext()
                {
                    while (updatesIter.hasNext())
                    {
                        Unfiltered update = updatesIter.next();
                        // If it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it for view updates
                        if (update.isRangeTombstoneMarker())
                            continue;

                        Row updateRow = (Row) update;
                        addToViewUpdateGenerators(emptyRow(updateRow.clustering(), existingsDeletion.currentDeletion()),
                                                  updateRow,
                                                  generators,
                                                  nowInSec);

                        // If the updates have been filtered, then we won't have any mutations; we need to make sure that we
                        // only return if the mutations are empty. Otherwise, we continue to search for an update which is
                        // not filtered
                        Collection<Mutation> mutations = buildMutations(baseTableMetadata, generators);
                        if (!mutations.isEmpty())
                            return mutations;
                    }

                    return null;
                }

                public boolean hasNext()
                {
                    return next != null;
                }

                public Collection<Mutation> next()
                {
                    Collection<Mutation> mutations = next;

                    next = buildNext();

                    assert !mutations.isEmpty() : "Expected mutations to be non-empty";
                    return mutations;
                }
            };
        }
        else
        {
            while (updatesIter.hasNext())
            {
                Unfiltered update = updatesIter.next();
                // If it's a range tombstone, it removes nothing pre-exisiting, so we can ignore it for view updates
                if (update.isRangeTombstoneMarker())
                    continue;

                Row updateRow = (Row) update;
                addToViewUpdateGenerators(emptyRow(updateRow.clustering(), existingsDeletion.currentDeletion()),
                                          updateRow,
                                          generators,
                                          nowInSec);
            }

            return Iterators.singletonIterator(buildMutations(baseTableMetadata, generators));
        }
    }

    /**
     * Return the views that are potentially updated by the provided updates.
     *
     * @param updates the updates applied to the base table.
     * @return the views affected by {@code updates}.
     */
    public Collection<View> updatedViews(PartitionUpdate updates)
    {
        List<View> matchingViews = new ArrayList<>(views.size());

        for (View view : views)
        {
            ReadQuery selectQuery = view.getReadQuery();
            if (!selectQuery.selectsKey(updates.partitionKey()))
                continue;

            matchingViews.add(view);
        }
        return matchingViews;
    }

    /**
     * Returns the command to use to read the existing rows required to generate view updates for the provided base
     * base updates.
     *
     * @param updates the base table updates being applied.
     * @param views the views potentially affected by {@code updates}.
     * @param nowInSec the current time in seconds.
     * @return the command to use to read the base table rows required to generate view updates for {@code updates}.
     */
    private SinglePartitionReadCommand readExistingRowsCommand(PartitionUpdate updates, Collection<View> views, int nowInSec)
    {
        Slices.Builder sliceBuilder = null;
        DeletionInfo deletionInfo = updates.deletionInfo();
        CFMetaData metadata = updates.metadata();
        DecoratedKey key = updates.partitionKey();
        // TODO: This is subtle: we need to gather all the slices that we have to fetch between partition del, range tombstones and rows.
        if (!deletionInfo.isLive())
        {
            sliceBuilder = new Slices.Builder(metadata.comparator);
            // Everything covered by a deletion might invalidate an existing view entry, which means we must read it to know. In practice
            // though, the views involved might filter some base table clustering columns, in which case we can restrict what we read
            // using those restrictions.
            // If there is a partition deletion, then we can simply take each slices from each view select filter. They may overlap but
            // the Slices.Builder handles that for us. Note that in many case this will just involve reading everything (as soon as any
            // view involved has no clustering restrictions for instance).
            // For range tombstone, we should theoretically take the difference between the range tombstoned and the slices selected
            // by every views, but as we don't an easy way to compute that right now, we keep it simple and just use the tombstoned
            // range.
            // TODO: we should improve that latter part.
            if (!deletionInfo.getPartitionDeletion().isLive())
            {
                for (View view : views)
                    sliceBuilder.addAll(view.getSelectStatement().clusteringIndexFilterAsSlices());
            }
            else
            {
                assert deletionInfo.hasRanges();
                Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
                while (iter.hasNext())
                    sliceBuilder.add(iter.next().deletedSlice());
            }
        }

        // We need to read every row that is updated, unless we can prove that it has no impact on any view entries.

        // If we had some slices from the deletions above, we'll continue using that. Otherwise, it's more efficient to build
        // a names query.
        BTreeSet.Builder<Clustering> namesBuilder = sliceBuilder == null ? BTreeSet.builder(metadata.comparator) : null;
        for (Row row : updates)
        {
            // Don't read the existing state if we can prove the update won't affect any views
            if (!affectsAnyViews(key, row, views))
                continue;

            if (namesBuilder == null)
                sliceBuilder.add(Slice.make(row.clustering()));
            else
                namesBuilder.add(row.clustering());
        }

        NavigableSet<Clustering> names = namesBuilder == null ? null : namesBuilder.build();
        // If we have a slice builder, it means we had some deletions and we have to read. But if we had
        // only row updates, it's possible none of them affected the views, in which case we have nothing
        // to do.
        if (names != null && names.isEmpty())
            return null;

        ClusteringIndexFilter clusteringFilter = names == null
                                               ? new ClusteringIndexSliceFilter(sliceBuilder.build(), false)
                                               : new ClusteringIndexNamesFilter(names, false);
        // since unselected columns also affect view liveness, we need to query all base columns if base and view have same key columns.
        // If we have more than one view, we should merge the queried columns by each views but to keep it simple we just
        // include everything. We could change that in the future.
        ColumnFilter queriedColumns = views.size() == 1 && metadata.enforceStrictLiveness()
                                   ? Iterables.getOnlyElement(views).getSelectStatement().queriedColumns()
                                   : ColumnFilter.all(metadata);
        // Note that the views could have restrictions on regular columns, but even if that's the case we shouldn't apply those
        // when we read, because even if an existing row doesn't match the view filter, the update can change that in which
        // case we'll need to know the existing content. There is also no easy way to merge those RowFilter when we have multiple views.
        // TODO: we could still make sense to special case for when there is a single view and a small number of updates (and
        // no deletions). Indeed, in that case we could check whether any of the update modify any of the restricted regular
        // column, and if that's not the case we could use view filter. We keep it simple for now though.
        RowFilter rowFilter = RowFilter.NONE;
        return SinglePartitionReadCommand.create(metadata, nowInSec, queriedColumns, rowFilter, DataLimits.NONE, key, clusteringFilter);
    }

    private boolean affectsAnyViews(DecoratedKey partitionKey, Row update, Collection<View> views)
    {
        for (View view : views)
        {
            if (view.mayBeAffectedBy(partitionKey, update))
                return true;
        }
        return false;
    }

    /**
     * Given an existing base row and the update that we're going to apply to this row, generate the modifications
     * to apply to MVs using the provided {@code ViewUpdateGenerator}s.
     *
     * @param existingBaseRow the base table row as it is before an update.
     * @param updateBaseRow the newly updates made to {@code existingBaseRow}.
     * @param generators the view update generators to add the new changes to.
     * @param nowInSec the current time in seconds. Used to decide if data is live or not.
     */
    private static void addToViewUpdateGenerators(Row existingBaseRow, Row updateBaseRow, Collection<ViewUpdateGenerator> generators, int nowInSec)
    {
        // Having existing empty is useful, it just means we'll insert a brand new entry for updateBaseRow,
        // but if we have no update at all, we shouldn't get there.
        assert !updateBaseRow.isEmpty();

        // We allow existingBaseRow to be null, which we treat the same as being empty as an small optimization
        // to avoid allocating empty row objects when we know there was nothing existing.
        Row mergedBaseRow = existingBaseRow == null ? updateBaseRow : Rows.merge(existingBaseRow, updateBaseRow, nowInSec);
        for (ViewUpdateGenerator generator : generators)
            generator.addBaseTableUpdate(existingBaseRow, mergedBaseRow);
    }

    private static Row emptyRow(Clustering clustering, DeletionTime deletion)
    {
        // Returning null for an empty row is slightly ugly, but the case where there is no pre-existing row is fairly common
        // (especially when building the view), so we want to avoid a dummy allocation of an empty row every time.
        // And MultiViewUpdateBuilder knows how to deal with that.
        return deletion.isLive() ? null : BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(deletion));
    }

    /**
     * Extracts (and potentially groups) the mutations generated by the provided view update generator.
     * Returns the mutation that needs to be done to the views given the base table updates
     * passed to {@link #addBaseTableUpdate}.
     *
     * @param baseTableMetadata the metadata for the base table being updated.
     * @param generators the generators from which to extract the view mutations from.
     * @return the mutations created by all the generators in {@code generators}.
     */
    private Collection<Mutation> buildMutations(CFMetaData baseTableMetadata, List<ViewUpdateGenerator> generators)
    {
        // One view is probably common enough and we can optimize a bit easily
        if (generators.size() == 1)
        {
            ViewUpdateGenerator generator = generators.get(0);
            Collection<PartitionUpdate> updates = generator.generateViewUpdates();
            List<Mutation> mutations = new ArrayList<>(updates.size());
            for (PartitionUpdate update : updates)
                mutations.add(new Mutation(update));

            generator.clear();
            return mutations;
        }

        Map<DecoratedKey, Mutation> mutations = new HashMap<>();
        for (ViewUpdateGenerator generator : generators)
        {
            for (PartitionUpdate update : generator.generateViewUpdates())
            {
                DecoratedKey key = update.partitionKey();
                Mutation mutation = mutations.get(key);
                if (mutation == null)
                {
                    mutation = new Mutation(baseTableMetadata.ksName, key);
                    mutations.put(key, mutation);
                }
                mutation.add(update);
            }
            generator.clear();
        }
        return mutations.values();
    }

    /**
     * A simple helper that tracks for a given {@code UnfilteredRowIterator} what is the current deletion at any time of the
     * iteration. It will be the currently open range tombstone deletion if there is one and the partition deletion otherwise.
     */
    private static class DeletionTracker
    {
        private final DeletionTime partitionDeletion;
        private DeletionTime deletion;

        public DeletionTracker(DeletionTime partitionDeletion)
        {
            this.partitionDeletion = partitionDeletion;
        }

        public void update(Unfiltered marker)
        {
            assert marker instanceof RangeTombstoneMarker;
            RangeTombstoneMarker rtm = (RangeTombstoneMarker)marker;
            this.deletion = rtm.isOpen(false)
                          ? rtm.openDeletionTime(false)
                          : null;
        }

        public DeletionTime currentDeletion()
        {
            return deletion == null ? partitionDeletion : deletion;
        }
    }
}
