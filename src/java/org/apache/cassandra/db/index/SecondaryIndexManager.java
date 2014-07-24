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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Manages all the indexes associated with a given CFS
 * Different types of indexes can be created across the same CF
 */
public class SecondaryIndexManager
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);

    public static final Updater nullUpdater = new Updater()
    {
        public void insert(Cell cell) { }

        public void update(Cell oldCell, Cell cell) { }

        public void remove(Cell current) { }

        public void updateRowLevelIndexes() {}
    };

    /**
     * Organizes the indexes by column name
     */
    private final ConcurrentNavigableMap<ByteBuffer, SecondaryIndex> indexesByColumn;


    /**
     * Keeps a single instance of a SecondaryIndex for many columns when the index type
     * has isRowLevelIndex() == true
     *
     * This allows updates to happen to an entire row at once
     */
    private final ConcurrentMap<Class<? extends SecondaryIndex>, SecondaryIndex> rowLevelIndexMap;


    /**
     * Keeps all secondary index instances, either per-column or per-row
     */
    private final Set<SecondaryIndex> allIndexes;


    /**
     * The underlying column family containing the source data for these indexes
     */
    public final ColumnFamilyStore baseCfs;

    public SecondaryIndexManager(ColumnFamilyStore baseCfs)
    {
        indexesByColumn = new ConcurrentSkipListMap<>();
        rowLevelIndexMap = new ConcurrentHashMap<>();
        allIndexes = Collections.newSetFromMap(new ConcurrentHashMap<SecondaryIndex, Boolean>());

        this.baseCfs = baseCfs;
    }

    /**
     * Drops and adds new indexes associated with the underlying CF
     */
    public void reload()
    {
        // figure out what needs to be added and dropped.
        // future: if/when we have modifiable settings for secondary indexes,
        // they'll need to be handled here.
        Collection<ByteBuffer> indexedColumnNames = indexesByColumn.keySet();
        for (ByteBuffer indexedColumn : indexedColumnNames)
        {
            ColumnDefinition def = baseCfs.metadata.getColumnDefinition(indexedColumn);
            if (def == null || def.getIndexType() == null)
                removeIndexedColumn(indexedColumn);
        }

        // TODO: allow all ColumnDefinition type
        for (ColumnDefinition cdef : baseCfs.metadata.allColumns())
            if (cdef.getIndexType() != null && !indexedColumnNames.contains(cdef.name.bytes))
                addIndexedColumn(cdef);

        for (SecondaryIndex index : allIndexes)
            index.reload();
    }

    public Set<String> allIndexesNames()
    {
        Set<String> names = new HashSet<>(allIndexes.size());
        for (SecondaryIndex index : allIndexes)
            names.add(index.getIndexName());
        return names;
    }

    /**
     * Does a full, blocking rebuild of the indexes specified by columns from the sstables.
     * Does nothing if columns is empty.
     *
     * Caller must acquire and release references to the sstables used here.
     *
     * @param sstables the data to build from
     * @param idxNames the list of columns to index, ordered by comparator
     */
    public void maybeBuildSecondaryIndexes(Collection<SSTableReader> sstables, Set<String> idxNames)
    {
        if (idxNames.isEmpty())
            return;

        logger.info(String.format("Submitting index build of %s for data in %s",
                                  idxNames, StringUtils.join(sstables, ", ")));

        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs, idxNames, new ReducingKeyIterator(sstables));
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        FBUtilities.waitOnFuture(future);

        flushIndexesBlocking();

        logger.info("Index build of {} complete", idxNames);
    }

    public boolean indexes(CellName name, Set<SecondaryIndex> indexes)
    {
        boolean matching = false;
        for (SecondaryIndex index : indexes)
        {
            if (index.indexes(name))
            {
                matching = true;
                break;
            }
        }
        return matching;
    }

    public Set<SecondaryIndex> indexFor(CellName name, Set<SecondaryIndex> indexes)
    {
        Set<SecondaryIndex> matching = null;
        for (SecondaryIndex index : indexes)
        {
            if (index.indexes(name))
            {
                if (matching == null)
                    matching = new HashSet<>();
                matching.add(index);
            }
        }
        return matching == null ? Collections.<SecondaryIndex>emptySet() : matching;
    }

    public boolean indexes(Cell cell)
    {
        return indexes(cell.name());
    }

    public boolean indexes(CellName name)
    {
        return indexes(name, allIndexes);
    }

    public Set<SecondaryIndex> indexFor(CellName name)
    {
        return indexFor(name, allIndexes);
    }

    /**
     * @return true if at least one of the indexes can handle the clause.
     */
    public boolean hasIndexFor(List<IndexExpression> clause)
    {
        if (clause == null || clause.isEmpty())
            return false;

        for (SecondaryIndexSearcher searcher : getIndexSearchersForQuery(clause))
            if (searcher.canHandleIndexClause(clause))
                return true;

        return false;
    }

    /**
     * Removes a existing index
     * @param column the indexed column to remove
     */
    public void removeIndexedColumn(ByteBuffer column)
    {
        SecondaryIndex index = indexesByColumn.remove(column);

        if (index == null)
            return;

        // Remove this column from from row level index map as well as all indexes set
        if (index instanceof PerRowSecondaryIndex)
        {
            index.removeColumnDef(column);

            // If no columns left remove from row level lookup as well as all indexes set
            if (index.getColumnDefs().isEmpty())
            {
                allIndexes.remove(index);
                rowLevelIndexMap.remove(index.getClass());
            }
        }
        else
        {
            allIndexes.remove(index);
        }

        index.removeIndex(column);
        SystemKeyspace.setIndexRemoved(baseCfs.metadata.ksName, index.getNameForSystemKeyspace(column));
    }

    /**
     * Adds and builds a index for a column
     * @param cdef the column definition holding the index data
     * @return a future which the caller can optionally block on signaling the index is built
     */
    public synchronized Future<?> addIndexedColumn(ColumnDefinition cdef)
    {
        if (indexesByColumn.containsKey(cdef.name.bytes))
            return null;

        assert cdef.getIndexType() != null;

        SecondaryIndex index;
        try
        {
            index = SecondaryIndex.createInstance(baseCfs, cdef);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }

        // Keep a single instance of the index per-cf for row level indexes
        // since we want all columns to be under the index
        if (index instanceof PerRowSecondaryIndex)
        {
            SecondaryIndex currentIndex = rowLevelIndexMap.get(index.getClass());

            if (currentIndex == null)
            {
                rowLevelIndexMap.put(index.getClass(), index);
                index.init();
            }
            else
            {
                index = currentIndex;
                index.addColumnDef(cdef);
                logger.info("Creating new index : {}",cdef);
            }
        }
        else
        {
            // TODO: We sould do better than throw a RuntimeException
            if (cdef.getIndexType() == IndexType.CUSTOM && index instanceof AbstractSimplePerColumnSecondaryIndex)
                throw new RuntimeException("Cannot use a subclass of AbstractSimplePerColumnSecondaryIndex as a CUSTOM index, as they assume they are CFS backed");
            index.init();
        }

        // link in indexedColumns. this means that writes will add new data to
        // the index immediately,
        // so we don't have to lock everything while we do the build. it's up to
        // the operator to wait
        // until the index is actually built before using in queries.
        indexesByColumn.put(cdef.name.bytes, index);

        // Add to all indexes set:
        allIndexes.add(index);

        // if we're just linking in the index to indexedColumns on an
        // already-built index post-restart, we're done
        if (index.isIndexBuilt(cdef.name.bytes))
            return null;

        return index.buildIndexAsync();
    }

    /**
     *
     * @param column the name of indexes column
     * @return the index
     */
    public SecondaryIndex getIndexForColumn(ByteBuffer column)
    {
        return indexesByColumn.get(column);
    }

    /**
     * Remove the index
     */
    public void invalidate()
    {
        for (SecondaryIndex index : allIndexes)
            index.invalidate();
    }

    /**
     * Flush all indexes to disk
     */
    public void flushIndexesBlocking()
    {
        // despatch flushes for all CFS backed indexes
        List<Future<?>> wait = new ArrayList<>();
        synchronized (baseCfs.getDataTracker())
        {
            for (SecondaryIndex index : allIndexes)
                if (index.getIndexCfs() != null)
                    wait.add(index.getIndexCfs().forceFlush());
        }

        // blockingFlush any non-CFS-backed indexes
        for (SecondaryIndex index : allIndexes)
            if (index.getIndexCfs() == null)
                index.forceBlockingFlush();

        // wait for the CFS-backed index flushes to complete
        FBUtilities.waitOnFutures(wait);
    }

    /**
     * @return all built indexes (ready to use)
     */
    public List<String> getBuiltIndexes()
    {
        List<String> indexList = new ArrayList<>();

        for (Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
        {
            SecondaryIndex index = entry.getValue();

            if (index.isIndexBuilt(entry.getKey()))
                indexList.add(entry.getValue().getIndexName());
        }

        return indexList;
    }

    /**
     * @return all CFS from indexes which use a backing CFS internally (KEYS)
     */
    public Set<ColumnFamilyStore> getIndexesBackedByCfs()
    {
        Set<ColumnFamilyStore> cfsList = new HashSet<>();

        for (SecondaryIndex index: allIndexes)
        {
            ColumnFamilyStore cfs = index.getIndexCfs();
            if (cfs != null)
                cfsList.add(cfs);
        }

        return cfsList;
    }

    /**
     * @return all indexes which do *not* use a backing CFS internally
     */
    public Set<SecondaryIndex> getIndexesNotBackedByCfs()
    {
        // we use identity map because per row indexes use same instance across many columns
        Set<SecondaryIndex> indexes = Collections.newSetFromMap(new IdentityHashMap<SecondaryIndex, Boolean>());
        for (SecondaryIndex index: allIndexes)
            if (index.getIndexCfs() == null)
                indexes.add(index);
        return indexes;
    }

    /**
     * @return all of the secondary indexes without distinction to the (non-)backed by secondary ColumnFamilyStore.
     */
    public Set<SecondaryIndex> getIndexes()
    {
        return allIndexes;
    }

    /**
     * @return if there are ANY indexes for this table..
     */
    public boolean hasIndexes()
    {
        return !indexesByColumn.isEmpty();
    }

    /**
     * When building an index against existing data, add the given row to the index
     *
     * @param key the row key
     * @param cf the current rows data
     */
    public void indexRow(ByteBuffer key, ColumnFamily cf, OpOrder.Group opGroup)
    {
        // Update entire row only once per row level index
        Set<Class<? extends SecondaryIndex>> appliedRowLevelIndexes = null;

        for (SecondaryIndex index : allIndexes)
        {
            if (index instanceof PerRowSecondaryIndex)
            {
                if (appliedRowLevelIndexes == null)
                    appliedRowLevelIndexes = new HashSet<>();

                if (appliedRowLevelIndexes.add(index.getClass()))
                    ((PerRowSecondaryIndex)index).index(key, cf);
            }
            else
            {
                for (Cell cell : cf)
                    if (index.indexes(cell.name()))
                        ((PerColumnSecondaryIndex) index).insert(key, cell, opGroup);
            }
        }
    }

    /**
     * Delete all columns from all indexes for this row.  For when cleanup rips a row out entirely.
     *
     * @param key the row key
     * @param indexedColumnsInRow all column names in row
     */
    public void deleteFromIndexes(DecoratedKey key, List<Cell> indexedColumnsInRow, OpOrder.Group opGroup)
    {
        // Update entire row only once per row level index
        Set<Class<? extends SecondaryIndex>> cleanedRowLevelIndexes = null;

        for (Cell cell : indexedColumnsInRow)
        {
            // TODO: this is probably incorrect, we should pull all indexes
            ColumnDefinition cDef = baseCfs.metadata.getColumnDefinition(cell.name());
            SecondaryIndex index = indexesByColumn.get(cDef.name.bytes);
            if (index == null)
                continue;

            if (index instanceof PerRowSecondaryIndex)
            {
                if (cleanedRowLevelIndexes == null)
                    cleanedRowLevelIndexes = new HashSet<>();

                if (cleanedRowLevelIndexes.add(index.getClass()))
                    ((PerRowSecondaryIndex)index).delete(key, opGroup);
            }
            else
            {
                ((PerColumnSecondaryIndex) index).delete(key.getKey(), cell, opGroup);
            }
        }
    }

    /**
     * This helper acts as a closure around the indexManager
     * and updated cf data to ensure that down in
     * Memtable's ColumnFamily implementation, the index
     * can get updated. Note: only a CF backed by AtomicSortedColumns implements
     * this behaviour fully, other types simply ignore the index updater.
     */
    public Updater updaterFor(DecoratedKey key, ColumnFamily cf, OpOrder.Group opGroup)
    {
        return (indexesByColumn.isEmpty() && rowLevelIndexMap.isEmpty())
                ? nullUpdater
                : new StandardUpdater(key, cf, opGroup);
    }

    /**
     * Updated closure with only the modified row key.
     */
    public Updater updaterFor(DecoratedKey key, OpOrder.Group opGroup)
    {
        return updaterFor(key, null, opGroup);
    }

    /**
     * Updated closure with only the modified row key.
     */
    public Updater gcUpdaterFor(DecoratedKey key)
    {
        return new GCUpdater(key);
    }

    /**
     * Get a list of IndexSearchers from the union of expression index types
     * @param clause the query clause
     * @return the searchers needed to query the index
     */
    public List<SecondaryIndexSearcher> getIndexSearchersForQuery(List<IndexExpression> clause)
    {
        Map<String, Set<ByteBuffer>> groupByIndexType = new HashMap<>();

        //Group columns by type
        for (IndexExpression ix : clause)
        {
            SecondaryIndex index = getIndexForColumn(ix.column);

            if (index == null)
                continue;

            Set<ByteBuffer> columns = groupByIndexType.get(index.getClass().getCanonicalName());

            if (columns == null)
            {
                columns = new HashSet<>();
                groupByIndexType.put(index.getClass().getCanonicalName(), columns);
            }

            columns.add(ix.column);
        }

        List<SecondaryIndexSearcher> indexSearchers = new ArrayList<>(groupByIndexType.size());

        //create searcher per type
        for (Set<ByteBuffer> column : groupByIndexType.values())
            indexSearchers.add(getIndexForColumn(column.iterator().next()).createSecondaryIndexSearcher(column));

        return indexSearchers;
    }

    /**
     * Performs a search across a number of column indexes
     *
     * @param filter the column range to restrict to
     * @return found indexed rows
     */
    public List<Row> search(ExtendedFilter filter)
    {
        List<SecondaryIndexSearcher> indexSearchers = getIndexSearchersForQuery(filter.getClause());

        if (indexSearchers.isEmpty())
            return Collections.emptyList();

        SecondaryIndexSearcher mostSelective = null;
        long bestEstimate = Long.MAX_VALUE;
        for (SecondaryIndexSearcher searcher : indexSearchers)
        {
            SecondaryIndex highestSelectivityIndex = searcher.highestSelectivityIndex(filter.getClause());
            long estimate = highestSelectivityIndex.estimateResultRows();
            if (estimate <= bestEstimate)
            {
                bestEstimate = estimate;
                mostSelective = searcher;
            }
        }

        return mostSelective.search(filter);
    }

    public Set<SecondaryIndex> getIndexesByNames(Set<String> idxNames)
    {
        Set<SecondaryIndex> result = new HashSet<>();
        for (SecondaryIndex index : allIndexes)
            if (idxNames.contains(index.getIndexName()))
                result.add(index);
        return result;
    }

    public void setIndexBuilt(Set<String> idxNames)
    {
        for (SecondaryIndex index : getIndexesByNames(idxNames))
            index.setIndexBuilt();
    }

    public void setIndexRemoved(Set<String> idxNames)
    {
        for (SecondaryIndex index : getIndexesByNames(idxNames))
            index.setIndexRemoved();
    }

    public boolean validate(Cell cell)
    {
        for (SecondaryIndex index : indexFor(cell.name()))
        {
            if (!index.validate(cell))
                return false;
        }
        return true;
    }

    static boolean shouldCleanupOldValue(Cell oldCell, Cell newCell)
    {
        // If any one of name/value/timestamp are different, then we
        // should delete from the index. If not, then we can infer that
        // at least one of the cells is an ExpiringColumn and that the
        // difference is in the expiry time. In this case, we don't want to
        // delete the old value from the index as the tombstone we insert
        // will just hide the inserted value.
        // Completely identical cells (including expiring columns with
        // identical ttl & localExpirationTime) will not get this far due
        // to the oldCell.equals(newColumn) in StandardUpdater.update
        return !oldCell.name().equals(newCell.name())
            || !oldCell.value().equals(newCell.value())
            || oldCell.timestamp() != newCell.timestamp();
    }

    public static interface Updater
    {
        /** called when constructing the index against pre-existing data */
        public void insert(Cell cell);

        /** called when updating the index from a memtable */
        public void update(Cell oldCell, Cell cell);

        /** called when lazy-updating the index during compaction (CASSANDRA-2897) */
        public void remove(Cell current);

        /** called after memtable updates are complete (CASSANDRA-5397) */
        public void updateRowLevelIndexes();
    }

    private final class GCUpdater implements Updater
    {
        private final DecoratedKey key;

        public GCUpdater(DecoratedKey key)
        {
            this.key = key;
        }

        public void insert(Cell cell)
        {
            throw new UnsupportedOperationException();
        }

        public void update(Cell oldCell, Cell newCell)
        {
            throw new UnsupportedOperationException();
        }

        public void remove(Cell cell)
        {
            if (!cell.isLive())
                return;

            for (SecondaryIndex index : indexFor(cell.name()))
            {
                if (index instanceof PerColumnSecondaryIndex)
                {
                    try (OpOrder.Group opGroup = baseCfs.keyspace.writeOrder.start())
                    {
                        ((PerColumnSecondaryIndex) index).delete(key.getKey(), cell, opGroup);
                    }
                }
            }
        }

        public void updateRowLevelIndexes()
        {
            for (SecondaryIndex index : rowLevelIndexMap.values())
                ((PerRowSecondaryIndex) index).index(key.getKey(), null);
        }
    }

    private final class StandardUpdater implements Updater
    {
        private final DecoratedKey key;
        private final ColumnFamily cf;
        private final OpOrder.Group opGroup;

        public StandardUpdater(DecoratedKey key, ColumnFamily cf, OpOrder.Group opGroup)
        {
            this.key = key;
            this.cf = cf;
            this.opGroup = opGroup;
        }

        public void insert(Cell cell)
        {
            if (!cell.isLive())
                return;

            for (SecondaryIndex index : indexFor(cell.name()))
                if (index instanceof PerColumnSecondaryIndex)
                    ((PerColumnSecondaryIndex) index).insert(key.getKey(), cell, opGroup);
        }

        public void update(Cell oldCell, Cell cell)
        {
            if (oldCell.equals(cell))
                return;
            
            for (SecondaryIndex index : indexFor(cell.name()))
            {
                if (index instanceof PerColumnSecondaryIndex)
                {
                    if (cell.isLive())
                    {
                        ((PerColumnSecondaryIndex) index).update(key.getKey(), oldCell, cell, opGroup);
                    }
                    else
                    {
                        // Usually we want to delete the old value from the index, except when
                        // name/value/timestamp are all equal, but the columns themselves
                        // are not (as is the case when overwriting expiring columns with
                        // identical values and ttl) Then, we don't want to delete as the
                        // tombstone will hide the new value we just inserted; see CASSANDRA-7268
                        if (shouldCleanupOldValue(oldCell, cell))
                            ((PerColumnSecondaryIndex) index).delete(key.getKey(), oldCell, opGroup);
                    }
                }
            }
        }

        public void remove(Cell cell)
        {
            if (!cell.isLive())
                return;

            for (SecondaryIndex index : indexFor(cell.name()))
                if (index instanceof PerColumnSecondaryIndex)
                   ((PerColumnSecondaryIndex) index).delete(key.getKey(), cell, opGroup);
        }

        public void updateRowLevelIndexes()
        {
            for (SecondaryIndex index : rowLevelIndexMap.values())
                ((PerRowSecondaryIndex) index).index(key.getKey(), cf);
        }

    }
}
