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
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Manages all the indexes associated with a given CFS
 * Different types of indexes can be created across the same CF
 */
public class SecondaryIndexManager
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);

    public static final Updater nullUpdater = new Updater()
    {
        public void insert(Column column) { }

        public void update(Column oldColumn, Column column) { }

        public void remove(Column current) { }

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
            if (cdef.getIndexType() != null && !indexedColumnNames.contains(cdef.name))
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

        logger.info("Index build of " + idxNames + " complete");
    }

    public boolean indexes(ByteBuffer name, Set<SecondaryIndex> indexes)
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

    public Set<SecondaryIndex> indexFor(ByteBuffer name, Set<SecondaryIndex> indexes)
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

    public boolean indexes(Column column)
    {
        return indexes(column.name());
    }

    public boolean indexes(ByteBuffer name)
    {
        return indexes(name, allIndexes);
    }

    public Set<SecondaryIndex> indexFor(ByteBuffer name)
    {
        return indexFor(name, allIndexes);
    }

    /**
     * @return true if the indexes can handle the clause.
     */
    public boolean hasIndexFor(List<IndexExpression> clause)
    {
        if (clause == null || clause.isEmpty())
            return false;

        // It doesn't seem a clause can have multiple searchers, but since
        // getIndexSearchersForQuery returns a list ...
        List<SecondaryIndexSearcher> searchers = getIndexSearchersForQuery(clause);
        if (searchers.isEmpty())
            return false;

        for (SecondaryIndexSearcher searcher : searchers)
            if (!searcher.isIndexing(clause))
                return false;

        return true;
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

        if (indexesByColumn.containsKey(cdef.name))
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
        indexesByColumn.put(cdef.name, index);

        // Add to all indexes set:
        allIndexes.add(index);

        // if we're just linking in the index to indexedColumns on an
        // already-built index post-restart, we're done
        if (index.isIndexBuilt(cdef.name))
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
        for (SecondaryIndex index : allIndexes)
            index.forceBlockingFlush();
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
     * @return total current ram size of all indexes
     */
    public long getTotalLiveSize()
    {
        long total = 0;
        for (SecondaryIndex index : getIndexes())
            total += index.getLiveSize();
        return total;
    }

    /**
     * When building an index against existing data, add the given row to the index
     *
     * @param key the row key
     * @param cf the current rows data
     */
    public void indexRow(ByteBuffer key, ColumnFamily cf)
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
                for (Column column : cf)
                    if (index.indexes(column.name()))
                        ((PerColumnSecondaryIndex) index).insert(key, column);
            }
        }
    }

    /**
     * Delete all columns from all indexes for this row.  For when cleanup rips a row out entirely.
     *
     * @param key the row key
     * @param indexedColumnsInRow all column names in row
     */
    public void deleteFromIndexes(DecoratedKey key, List<Column> indexedColumnsInRow)
    {
        // Update entire row only once per row level index
        Set<Class<? extends SecondaryIndex>> cleanedRowLevelIndexes = null;

        for (Column column : indexedColumnsInRow)
        {
            ColumnDefinition cDef = baseCfs.metadata.getColumnDefinitionFromColumnName(column.name());
            SecondaryIndex index = indexesByColumn.get(cDef.name);
            if (index == null)
                continue;

            if (index instanceof PerRowSecondaryIndex)
            {
                if (cleanedRowLevelIndexes == null)
                    cleanedRowLevelIndexes = new HashSet<>();

                if (cleanedRowLevelIndexes.add(index.getClass()))
                    ((PerRowSecondaryIndex)index).delete(key);
            }
            else
            {
                ((PerColumnSecondaryIndex) index).delete(key.key, column);
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
    public Updater updaterFor(DecoratedKey key, ColumnFamily cf)
    {
        return (indexesByColumn.isEmpty() && rowLevelIndexMap.isEmpty())
                ? nullUpdater
                : new StandardUpdater(key, cf);
    }

    /**
     * Updated closure with only the modified row key.
     */
    public Updater updaterFor(DecoratedKey key)
    {
        return updaterFor(key, null);
    }

    /**
     * Get a list of IndexSearchers from the union of expression index types
     * @param clause the query clause
     * @return the searchers needed to query the index
     */
    private List<SecondaryIndexSearcher> getIndexSearchersForQuery(List<IndexExpression> clause)
    {
        Map<String, Set<ByteBuffer>> groupByIndexType = new HashMap<>();

        //Group columns by type
        for (IndexExpression ix : clause)
        {
            SecondaryIndex index = getIndexForColumn(ix.column_name);

            if (index == null)
                continue;

            Set<ByteBuffer> columns = groupByIndexType.get(index.getClass().getCanonicalName());

            if (columns == null)
            {
                columns = new HashSet<>();
                groupByIndexType.put(index.getClass().getCanonicalName(), columns);
            }

            columns.add(ix.column_name);
        }

        List<SecondaryIndexSearcher> indexSearchers = new ArrayList<>(groupByIndexType.size());

        //create searcher per type
        for (Set<ByteBuffer> column : groupByIndexType.values())
            indexSearchers.add(getIndexForColumn(column.iterator().next()).createSecondaryIndexSearcher(column));

        return indexSearchers;
    }

    /**
     * Performs a search across a number of column indexes
     * TODO: add support for querying across index types
     *
     * @param filter the column range to restrict to
     * @return found indexed rows
     */
    public List<Row> search(ExtendedFilter filter)
    {
        List<SecondaryIndexSearcher> indexSearchers = getIndexSearchersForQuery(filter.getClause());

        if (indexSearchers.isEmpty())
            return Collections.emptyList();

        //We currently don't support searching across multiple index types
        if (indexSearchers.size() > 1)
            throw new RuntimeException("Unable to search across multiple secondary index types");

        return indexSearchers.get(0).search(filter);
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

    public boolean validate(Column column)
    {
        SecondaryIndex index = getIndexForColumn(column.name());
        return index == null || index.validate(column);
    }

    public static interface Updater
    {
        /** called when constructing the index against pre-existing data */
        public void insert(Column column);

        /** called when updating the index from a memtable */
        public void update(Column oldColumn, Column column);

        /** called when lazy-updating the index during compaction (CASSANDRA-2897) */
        public void remove(Column current);

        /** called after memtable updates are complete (CASSANDRA-5397) */
        public void updateRowLevelIndexes();
    }

    private class StandardUpdater implements Updater
    {
        private final DecoratedKey key;
        private final ColumnFamily cf;

        public StandardUpdater(DecoratedKey key, ColumnFamily cf)
        {
            this.key = key;
            this.cf = cf;
        }

        public void insert(Column column)
        {
            if (column.isMarkedForDelete(System.currentTimeMillis()))
                return;

            for (SecondaryIndex index : indexFor(column.name()))
                if (index instanceof PerColumnSecondaryIndex)
                    ((PerColumnSecondaryIndex) index).insert(key.key, column);
        }

        public void update(Column oldColumn, Column column)
        {
            if (oldColumn.equals(column))
                return;
            
            for (SecondaryIndex index : indexFor(column.name()))
            {
                if (index instanceof PerColumnSecondaryIndex)
                {
                    // insert the new value before removing the old one, so we never have a period
                    // where the row is invisible to both queries (the opposite seems preferable); see CASSANDRA-5540
                    if (!column.isMarkedForDelete(System.currentTimeMillis()))
                        ((PerColumnSecondaryIndex) index).insert(key.key, column);
                    ((PerColumnSecondaryIndex) index).delete(key.key, oldColumn);
                }
            }
        }

        public void remove(Column column)
        {
            if (column.isMarkedForDelete(System.currentTimeMillis()))
                return;

            for (SecondaryIndex index : indexFor(column.name()))
                if (index instanceof PerColumnSecondaryIndex)
                   ((PerColumnSecondaryIndex) index).delete(key.key, column);
        }

        public void updateRowLevelIndexes()
        {
            for (SecondaryIndex index : rowLevelIndexMap.values())
                ((PerRowSecondaryIndex) index).index(key.key, cf);
        }
    }
}
