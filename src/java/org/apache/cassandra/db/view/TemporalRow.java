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

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Represents a single CQL Row in a base table, with both the currently persisted value and the update's value. The
 * values are stored in timestamp order, but also indicate whether they are from the currently persisted, allowing a
 * {@link TemporalRow.Resolver} to resolve if the value is an old value that has been updated; if it sorts after the
 * update's value, then it does not qualify.
 */
public class TemporalRow
{
    private static final int NO_TTL = LivenessInfo.NO_TTL;
    private static final long NO_TIMESTAMP = LivenessInfo.NO_TIMESTAMP;
    private static final int NO_DELETION_TIME = DeletionTime.LIVE.localDeletionTime();

    public interface Resolver
    {
        /**
         * @param cellVersions  all cells for a certain TemporalRow's Cell
         * @return      A single TemporalCell from the iterable which satisfies the resolution criteria, or null if
         *              there is no cell which qualifies
         */
        TemporalCell resolve(TemporalCell.Versions cellVersions);
    }

    public static final Resolver oldValueIfUpdated = TemporalCell.Versions::getOldCellIfUpdated;
    public static final Resolver earliest = TemporalCell.Versions::getEarliestCell;
    public static final Resolver latest = TemporalCell.Versions::getLatestCell;

    private static class TemporalCell
    {
        public final ByteBuffer value;
        public final long timestamp;
        public final int ttl;
        public final int localDeletionTime;
        public final boolean isNew;

        private TemporalCell(ByteBuffer value, long timestamp, int ttl, int localDeletionTime, boolean isNew)
        {
            this.value = value;
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.localDeletionTime = localDeletionTime;
            this.isNew = isNew;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("value", value == null ? "null" : ByteBufferUtil.bytesToHex(value))
                    .add("timestamp", timestamp)
                    .add("ttl", ttl)
                    .add("localDeletionTime", localDeletionTime)
                    .add("isNew", isNew)
                    .toString();
        }

        public TemporalCell reconcile(TemporalCell that)
        {
            int now = FBUtilities.nowInSeconds();
            Conflicts.Resolution resolution = Conflicts.resolveRegular(that.timestamp,
                                                                       that.isLive(now),
                                                                       that.localDeletionTime,
                                                                       that.value,
                                                                       this.timestamp,
                                                                       this.isLive(now),
                                                                       this.localDeletionTime,
                                                                       this.value);
            assert resolution != Conflicts.Resolution.MERGE;
            if (resolution == Conflicts.Resolution.LEFT_WINS)
                return that;
            return this;
        }

        private boolean isLive(int now)
        {
            return localDeletionTime == NO_DELETION_TIME || (ttl != NO_TTL && now < localDeletionTime);
        }

        public Cell cell(ColumnDefinition definition, CellPath cellPath)
        {
            return new BufferCell(definition, timestamp, ttl, localDeletionTime, value, cellPath);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TemporalCell that = (TemporalCell) o;

            if (timestamp != that.timestamp) return false;
            if (ttl != that.ttl) return false;
            if (localDeletionTime != that.localDeletionTime) return false;
            if (isNew != that.isNew) return false;
            return !(value != null ? !value.equals(that.value) : that.value != null);
        }

        public int hashCode()
        {
            int result = value != null ? value.hashCode() : 0;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + ttl;
            result = 31 * result + localDeletionTime;
            result = 31 * result + (isNew ? 1 : 0);
            return result;
        }

        /**
         * Tracks the versions of a cell for a given TemporalRow.
         * There are only two possible versions, existing and new.
         *
         */
        static class Versions
        {
            private TemporalCell existingCell = null;
            private TemporalCell newCell = null;
            private int numSet = 0;


            /**
             * @return the cell that is earliest
             * Or would be overwritten in the case of a timestamp conflict
             */
            public TemporalCell getEarliestCell()
            {
                assert numSet > 0;

                if (numSet == 1)
                    return existingCell == null ? newCell : existingCell;

                TemporalCell latest = existingCell.reconcile(newCell);

                return latest == newCell ? existingCell : newCell;
            }

            /**
             * @return the cell that is latest
             * Or would be the winner in the case of a timestamp conflict
             */
            public TemporalCell getLatestCell()
            {
                assert numSet > 0;

                if (numSet == 1)
                    return existingCell == null ? newCell : existingCell;

                return existingCell.reconcile(newCell);
            }

            /**
             * @return the new cell if it updates the existing cell
             */
            public TemporalCell getOldCellIfUpdated()
            {
                assert numSet > 0;

                if (numSet == 1)
                   return null;

                TemporalCell value = existingCell.reconcile(newCell);

                return ByteBufferUtil.compareUnsigned(existingCell.value, value.value) != 0 ? existingCell : null;
            }

            void setVersion(TemporalCell cell)
            {
                assert cell != null;

                if (cell.isNew)
                {
                    assert newCell == null || newCell.equals(cell) : "Only one cell version can be marked New; newCell: " + newCell + ", cell: " + cell;
                    newCell = cell;
                    numSet = existingCell == null ? 1 : 2;
                }
                else
                {
                    assert existingCell == null || existingCell.equals(cell) : "Only one cell version can be marked Existing; existingCell: " + existingCell + ", cell: " + cell;
                    existingCell = cell;
                    numSet = newCell == null ? 1 : 2;
                }
            }

            public void addToRow(TemporalRow row, ColumnIdentifier column, CellPath path)
            {
                if (existingCell != null)
                    row.addColumnValue(column, path, existingCell.timestamp, existingCell.ttl,
                                       existingCell.localDeletionTime, existingCell.value, existingCell.isNew);

                if (newCell != null)
                    row.addColumnValue(column, path, newCell.timestamp, newCell.ttl,
                                       newCell.localDeletionTime, newCell.value, newCell.isNew);
            }

            @Override
            public String toString()
            {
                return MoreObjects.toStringHelper(this)
                        .add("numSet", numSet)
                        .add("existingCell", existingCell)
                        .add("newCell", newCell)
                        .toString();
            }
        }
    }

    private final ColumnFamilyStore baseCfs;
    private final java.util.Set<ColumnIdentifier> viewPrimaryKey;
    private final ByteBuffer basePartitionKey;
    private final Map<ColumnIdentifier, ByteBuffer> clusteringColumns;
    private final Row startRow;
    private final boolean startIsNew;

    public final int nowInSec;
    private final Map<ColumnIdentifier, Map<CellPath, TemporalCell.Versions>> columnValues = new HashMap<>();
    private int viewClusteringTtl = NO_TTL;
    private long viewClusteringTimestamp = NO_TIMESTAMP;
    private int viewClusteringLocalDeletionTime = NO_DELETION_TIME;

    TemporalRow(ColumnFamilyStore baseCfs, java.util.Set<ColumnIdentifier> viewPrimaryKey, ByteBuffer key, Row row, int nowInSec, boolean isNew)
    {
        this.baseCfs = baseCfs;
        this.viewPrimaryKey = viewPrimaryKey;
        this.basePartitionKey = key;
        this.startRow = row;
        this.startIsNew = isNew;
        this.nowInSec = nowInSec;

        LivenessInfo liveness = row.primaryKeyLivenessInfo();
        updateLiveness(liveness.ttl(), liveness.timestamp(), row.deletion().time().localDeletionTime());

        List<ColumnDefinition> clusteringDefs = baseCfs.metadata.clusteringColumns();
        clusteringColumns = new HashMap<>();

        for (int i = 0; i < clusteringDefs.size(); i++)
        {
            ColumnDefinition cdef = clusteringDefs.get(i);
            clusteringColumns.put(cdef.name, row.clustering().get(i));

            addColumnValue(cdef.name, null, NO_TIMESTAMP, NO_TTL, NO_DELETION_TIME, row.clustering().get(i), isNew);
        }
    }

    /*
     * PK ts:5, ttl:1, deletion: 2
     * Col ts:4, ttl:2, deletion: 3
     *
     * TTL use min, since it expires at the lowest time which we are expiring. If we have the above values, we
     * would want to return 1, since the base row expires in 1 second.
     *
     * Timestamp uses max, as this is the time that the row has been written to the view. See CASSANDRA-10910.
     *
     * Local Deletion Time should use max, as this deletion will cover all previous values written.
     */
    private void updateLiveness(int ttl, long timestamp, int localDeletionTime)
    {
        // We are returning whichever is higher from valueIfSet
        // Natural order will return the max: 1.compareTo(2) < 0, so 2 is returned
        // Reverse order will return the min: 1.compareTo(2) > 0, so 1 is returned
        this.viewClusteringTtl = valueIfSet(viewClusteringTtl, ttl, NO_TTL, reverseOrder());
        this.viewClusteringTimestamp = valueIfSet(viewClusteringTimestamp, timestamp, NO_TIMESTAMP, naturalOrder());
        this.viewClusteringLocalDeletionTime = valueIfSet(viewClusteringLocalDeletionTime, localDeletionTime, NO_DELETION_TIME, naturalOrder());
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("table", baseCfs.keyspace.getName() + "." + baseCfs.getTableName())
                .add("basePartitionKey", ByteBufferUtil.bytesToHex(basePartitionKey))
                .add("startRow", startRow.toString(baseCfs.metadata))
                .add("startIsNew", startIsNew)
                .add("nowInSec", nowInSec)
                .add("viewClusteringTtl", viewClusteringTtl)
                .add("viewClusteringTimestamp", viewClusteringTimestamp)
                .add("viewClusteringLocalDeletionTime", viewClusteringLocalDeletionTime)
                .add("columnValues", columnValues)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TemporalRow that = (TemporalRow) o;

        if (!clusteringColumns.equals(that.clusteringColumns)) return false;
        if (!basePartitionKey.equals(that.basePartitionKey)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = basePartitionKey.hashCode();
        result = 31 * result + clusteringColumns.hashCode();
        return result;
    }

    public void addColumnValue(ColumnIdentifier identifier,
                               CellPath cellPath,
                               long timestamp,
                               int ttl,
                               int localDeletionTime,
                               ByteBuffer value,  boolean isNew)
    {
        if (!columnValues.containsKey(identifier))
            columnValues.put(identifier, new HashMap<>());

        Map<CellPath, TemporalCell.Versions> innerMap = columnValues.get(identifier);

        if (!innerMap.containsKey(cellPath))
            innerMap.put(cellPath, new TemporalCell.Versions());

        // If this column is part of the view's primary keys
        if (viewPrimaryKey.contains(identifier))
        {
            updateLiveness(ttl, timestamp, localDeletionTime);
        }

        innerMap.get(cellPath).setVersion(new TemporalCell(value, timestamp, ttl, localDeletionTime, isNew));
    }

    /**
     * @return
     * <ul>
     *     <li>
     *         If both existing and update are defaultValue, return defaultValue
     *     </li>
     *     <li>
     *         If only one of existing or existing are defaultValue, return the one which is not
     *     </li>
     *     <li>
     *         If both existing and update are not defaultValue, compare using comparator and return the higher one.
     *     </li>
     * </ul>
     */
    private static <T> T valueIfSet(T existing, T update, T defaultValue, Comparator<T> comparator)
    {
        if (existing.equals(defaultValue))
            return update;
        if (update.equals(defaultValue))
            return existing;
        return comparator.compare(existing, update) > 0 ? existing : update;
    }

    public int viewClusteringTtl()
    {
        return viewClusteringTtl;
    }

    public long viewClusteringTimestamp()
    {
        return viewClusteringTimestamp;
    }

    public int viewClusteringLocalDeletionTime()
    {
        return viewClusteringLocalDeletionTime;
    }

    public void addCell(Cell cell, boolean isNew)
    {
        addColumnValue(cell.column().name, cell.path(), cell.timestamp(), cell.ttl(), cell.localDeletionTime(), cell.value(), isNew);
    }

    // The Definition here is actually the *base table* definition
    public ByteBuffer clusteringValue(ColumnDefinition definition, Resolver resolver)
    {
        ColumnDefinition baseDefinition = definition.cfName.equals(baseCfs.name)
                                          ? definition
                                          : baseCfs.metadata.getColumnDefinition(definition.name);

        if (baseDefinition.isPartitionKey())
        {
            if (baseCfs.metadata.getKeyValidator() instanceof CompositeType)
            {
                CompositeType keyComparator = (CompositeType) baseCfs.metadata.getKeyValidator();
                ByteBuffer[] components = keyComparator.split(basePartitionKey);
                return components[baseDefinition.position()];
            }
            else
            {
                return basePartitionKey;
            }
        }
        else
        {
            ColumnIdentifier columnIdentifier = baseDefinition.name;

            if (clusteringColumns.containsKey(columnIdentifier))
                return clusteringColumns.get(columnIdentifier);

            Collection<org.apache.cassandra.db.rows.Cell> val = values(definition, resolver);
            if (val != null && val.size() == 1)
            {
                org.apache.cassandra.db.rows.Cell cell = Iterables.getOnlyElement(val);
                // handle single-column deletions correctly
                return cell.isTombstone() ? null : cell.value();
            }
        }
        return null;
    }

    public DeletionTime deletionTime(AbstractBTreePartition partition)
    {
        DeletionInfo deletionInfo = partition.deletionInfo();
        if (!deletionInfo.getPartitionDeletion().isLive())
            return deletionInfo.getPartitionDeletion();

        Clustering baseClustering = baseClusteringBuilder().build();
        RangeTombstone clusterTombstone = deletionInfo.rangeCovering(baseClustering);
        if (clusterTombstone != null)
            return clusterTombstone.deletionTime();

        Row row = partition.getRow(baseClustering);
        return row == null || row.deletion().isLive() ? DeletionTime.LIVE : row.deletion().time();
    }

    public Collection<org.apache.cassandra.db.rows.Cell> values(ColumnDefinition definition, Resolver resolver)
    {
        Map<CellPath, TemporalCell.Versions> innerMap = columnValues.get(definition.name);
        if (innerMap == null)
        {
            return Collections.emptyList();
        }

        Collection<org.apache.cassandra.db.rows.Cell> value = new ArrayList<>();
        for (Map.Entry<CellPath, TemporalCell.Versions> pathAndCells : innerMap.entrySet())
        {
            TemporalCell cell = resolver.resolve(pathAndCells.getValue());

            if (cell != null)
                value.add(cell.cell(definition, pathAndCells.getKey()));
        }
        return value;
    }

    public Slice baseSlice()
    {
        return baseClusteringBuilder().buildSlice();
    }

    private CBuilder baseClusteringBuilder()
    {
        CFMetaData metadata = baseCfs.metadata;
        CBuilder builder = CBuilder.create(metadata.comparator);

        ByteBuffer[] buffers = new ByteBuffer[clusteringColumns.size()];
        for (Map.Entry<ColumnIdentifier, ByteBuffer> buffer : clusteringColumns.entrySet())
            buffers[metadata.getColumnDefinition(buffer.getKey()).position()] = buffer.getValue();

        for (ByteBuffer byteBuffer : buffers)
            builder = builder.add(byteBuffer);

        return builder;
    }

    public Clustering baseClustering()
    {
        return startRow.clustering();
    }

    static class Set implements Iterable<TemporalRow>
    {
        private final ColumnFamilyStore baseCfs;
        private final java.util.Set<ColumnIdentifier> viewPrimaryKey;
        private final ByteBuffer key;
        public final DecoratedKey dk;
        private final Map<Clustering, TemporalRow> clusteringToRow;
        final int nowInSec = FBUtilities.nowInSeconds();
        private boolean hasTombstonedExisting = false;

        Set(ColumnFamilyStore baseCfs, java.util.Set<ColumnIdentifier> viewPrimaryKey, ByteBuffer key)
        {
            this.baseCfs = baseCfs;
            this.viewPrimaryKey = viewPrimaryKey;
            this.key = key;
            this.dk = baseCfs.decorateKey(key);
            this.clusteringToRow = new HashMap<>();
        }

        public Iterator<TemporalRow> iterator()
        {
            return clusteringToRow.values().iterator();
        }

        public TemporalRow getClustering(Clustering clustering)
        {
            return clusteringToRow.get(clustering);
        }

        public void addRow(Row row, boolean isNew)
        {
            TemporalRow temporalRow = clusteringToRow.get(row.clustering());
            if (temporalRow == null)
            {
                temporalRow = new TemporalRow(baseCfs, viewPrimaryKey, key, row, nowInSec, isNew);
                clusteringToRow.put(row.clustering(), temporalRow);
            }

            for (Cell cell : row.cells())
            {
                temporalRow.addCell(cell, isNew);
            }
        }

        private void addRow(TemporalRow row)
        {
            TemporalRow newRow = new TemporalRow(baseCfs, viewPrimaryKey, key, row.startRow, nowInSec, row.startIsNew);

            TemporalRow existing = clusteringToRow.put(row.startRow.clustering(), newRow);
            assert existing == null;

            for (Map.Entry<ColumnIdentifier, Map<CellPath, TemporalCell.Versions>> entry : row.columnValues.entrySet())
            {
                for (Map.Entry<CellPath, TemporalCell.Versions> cellPathEntry : entry.getValue().entrySet())
                {
                    TemporalCell.Versions cellVersions = cellPathEntry.getValue();

                    cellVersions.addToRow(newRow, entry.getKey(), cellPathEntry.getKey());
                }
            }
        }

        public TemporalRow.Set withNewViewPrimaryKey(java.util.Set<ColumnIdentifier> viewPrimaryKey)
        {
            TemporalRow.Set newSet = new Set(baseCfs, viewPrimaryKey, key);

            for (TemporalRow row : this)
                newSet.addRow(row);

            return newSet;
        }

        public boolean hasTombstonedExisting()
        {
            return hasTombstonedExisting;
        }

        public void setTombstonedExisting()
        {
            hasTombstonedExisting = true;
        }

        public int size()
        {
            return clusteringToRow.size();
        }
    }
}
