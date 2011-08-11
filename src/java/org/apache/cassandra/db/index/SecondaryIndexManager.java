/**
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.LocalToken;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexType;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages all the indexes associated with a given CFS
 * Different types of indexes can be created across the same CF
 */
public class SecondaryIndexManager
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);
    
    /**
     * Organized the indexes by column name
     */
    private final ConcurrentNavigableMap<ByteBuffer, SecondaryIndex> indexesByColumn;

    /**
     * The underlying column family containing the source data for these indexes
     */
    public final ColumnFamilyStore baseCfs;

    public SecondaryIndexManager(ColumnFamilyStore baseCfs)
    {
        indexesByColumn = new ConcurrentSkipListMap<ByteBuffer, SecondaryIndex>();

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
        Collection<ByteBuffer> indexedColumnNames = getIndexedColumns();
        for (ByteBuffer indexedColumn : indexedColumnNames)
        {
            ColumnDefinition def = baseCfs.metadata.getColumn_metadata().get(indexedColumn);
            if (def == null || def.getIndexType() == null)
                removeIndexedColumn(indexedColumn);
        }

        for (ColumnDefinition cdef : baseCfs.metadata.getColumn_metadata().values())
            if (cdef.getIndexType() != null && !indexedColumnNames.contains(cdef.name))
                addIndexedColumn(cdef);
    }
    
    
    /**
     * Does a full rebuild of the indexes specified by columns from the sstables
     * @param sstables the data to build from
     * @param columns the list of columns to index
     */
    public void buildSecondaryIndexes(Collection<SSTableReader> sstables, SortedSet<ByteBuffer> columns)
    {
        logger.info(String.format("Submitting index build of %s for data in %s",
                                  baseCfs.metadata.comparator.getString(columns), StringUtils.join(sstables, ", ")));

        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs, columns, new ReducingKeyIterator(sstables));
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        try
        {
            future.get();
            flushIndexes();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        logger.info("Index build of " + baseCfs.metadata.comparator.getString(columns) + " complete");
    }

    /**
     * @return the list of indexed columns
     */
    public SortedSet<ByteBuffer> getIndexedColumns()
    {
        return indexesByColumn.keySet();
    }

    /**
     * Removes a existing index
     * @param column the indexed column to remove
     */
    public void removeIndexedColumn(ByteBuffer column)
    {
        SecondaryIndex index = indexesByColumn.remove(column);
        
        if(index == null)
            return;
               
        SystemTable.setIndexRemoved(baseCfs.metadata.ksName, index.getIndexName());              
        
        index.removeIndex();      
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
        logger.info("Creating new index : {}",cdef);
        
        SecondaryIndex index = null;
        switch (cdef.getIndexType())
        {
        case KEYS:
            index = new KeysIndex(baseCfs, cdef);
            break;
        default:
            throw new RuntimeException("Unknown index type: " + cdef.getIndexName());
        }

        // link in indexedColumns. this means that writes will add new data to
        // the index immediately,
        // so we don't have to lock everything while we do the build. it's up to
        // the operator to wait
        // until the index is actually built before using in queries.
        if (indexesByColumn.putIfAbsent(cdef.name, index) != null)
            return null;       
        
        
        // if we're just linking in the index to indexedColumns on an
        // already-built index post-restart, we're done
        if (index.isIndexBuilt())
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
     * Remove all index MBeans
     */
    public void unregisterMBeans()
    {
        for(Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
            entry.getValue().unregisterMbean();
    }
    
    /**
     * Remove all underlying index data
     */
    public void removeAllIndexes()
    {
        for(Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
            entry.getValue().removeIndex();
    }
    
    /**
     * Rename all underlying index files
     * @param newCfName the new index Name
     */
    public void renameIndexes(String newCfName) throws IOException
    {
        for(Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
            entry.getValue().renameIndex(newCfName);
    }
    
    /**
     * Flush all indexes to disk
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void flushIndexes() throws ExecutionException, InterruptedException
    {
        for(Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
            entry.getValue().forceBlockingFlush();
    }
    
    /**
     * Returns the decoratedKey for a column value
     * @param name column name
     * @param value column value
     * @return decorated key
     */
    public DecoratedKey<LocalToken> getIndexKeyFor(ByteBuffer name, ByteBuffer value)
    {
        return new DecoratedKey<LocalToken>(new LocalToken(baseCfs.metadata.getColumnDefinition(name).getValidator(), value), value);
    }
    
    /**
     * Deletes data from one of the indexes
     * @param rowKey the row identifier to delete
     * @param column the column data to delete
     */
    public void deleteFromIndex(ByteBuffer rowKey, IColumn column)
    {
       SecondaryIndex index = indexesByColumn.get(column.name());
       
       if(index == null)
           return;
            
       DecoratedKey<LocalToken> valueKey = getIndexKeyFor(column.name(), column.value());

       index.deleteColumn(valueKey, rowKey, column);
    }
    
    /**
     * @return all built indexes (ready to use)
     */
    public List<String> getBuiltIndexes()
    {
        List<String> indexList = new ArrayList<String>();
        
        for(Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
        {
            if(entry.getValue().isIndexBuilt())
            {
                indexList.add(entry.getValue().getIndexName());
            }
        }
        
        return indexList;
    }
    
    /**
     * @return all CFS from indexes which use a backing CFS internally (KEYS)
     */
    public Collection<ColumnFamilyStore> getIndexesBackedByCfs()
    {
        ArrayList<ColumnFamilyStore> cfsList = new ArrayList<ColumnFamilyStore>();

        for(Map.Entry<ByteBuffer, SecondaryIndex> entry : indexesByColumn.entrySet())
        {
            ColumnFamilyStore cfs = entry.getValue().getUnderlyingCfs();
            
            if(cfs != null)
                cfsList.add(cfs);        
        }
        
        return cfsList;
    }
        
    /**
     * removes obsolete index entries and creates new ones for the given row key
     * and mutated columns.
     * 
     * @return list of full (index CF) memtables
     */
    public Set<SecondaryIndex> applyIndexUpdates(ByteBuffer rowKey,
                                                 ColumnFamily cf,
                                                 SortedSet<ByteBuffer> mutatedIndexedColumns,
                                                 ColumnFamily oldIndexedColumns)
    {
        
        //Track the indexes we touch so we can commit the row across them
        Set<SecondaryIndex> indexesTouched = new HashSet<SecondaryIndex>(indexesByColumn.size());
        
        // remove the old index entries
        if (oldIndexedColumns != null)
        {
            for (ByteBuffer columnName : oldIndexedColumns.getColumnNames())
            {
                
                IColumn column = oldIndexedColumns.getColumn(columnName);
                
                if (column == null)
                    continue;
                
                //this was previously deleted so should not be in index
                if (column.isMarkedForDelete())
                    continue;
           
                SecondaryIndex index = getIndexForColumn(columnName);
                assert index != null;
                
                indexesTouched.add(index);

                DecoratedKey<LocalToken> valueKey = getIndexKeyFor(columnName, column.value());

                index.deleteColumn(valueKey, rowKey, column);
            }           
        }
        
        //insert new columns
        for (ByteBuffer columnName : mutatedIndexedColumns)
        {
            IColumn column = cf.getColumn(columnName);
            if (column == null || column.isMarkedForDelete())
                continue; // null column == row deletion

            SecondaryIndex index = getIndexForColumn(columnName);
            assert index != null;

            indexesTouched.add(index);
            
            DecoratedKey<LocalToken> valueKey = getIndexKeyFor(columnName, column.value());
                        
            index.insertColumn(valueKey, rowKey, column);         
        }
        
        //Commit the row across all indexes
        for (SecondaryIndex index : indexesTouched)
        {
            index.commitRow(rowKey);
        }
        
        return indexesTouched;
    }
     
    /**
     * Get a list of IndexSearchers from the union of expression index types
     * @param clause the query clause
     * @return the searchers to needed to query the index
     */
    private List<SecondaryIndexSearcher> getIndexSearchersForQuery(IndexClause clause)
    {
        List<SecondaryIndexSearcher> indexSearchers = new ArrayList<SecondaryIndexSearcher>();
        
        Map<IndexType, Set<ByteBuffer>> groupByIndexType = new HashMap<IndexType, Set<ByteBuffer>>();
 
        
        //Group columns by type
        for(IndexExpression ix : clause.expressions)
        {
            SecondaryIndex index = getIndexForColumn(ix.column_name);
            
            if(index == null)
                continue;
            
            Set<ByteBuffer> columns = groupByIndexType.get(index.type());
            
            if (columns == null)
            {
                columns = new HashSet<ByteBuffer>();
                groupByIndexType.put(index.type(), columns);
            }
            
            columns.add(ix.column_name);        
        }
        
        //create searcher per type
        for (Map.Entry<IndexType, Set<ByteBuffer>> entry : groupByIndexType.entrySet())
        {
            indexSearchers.add( getIndexForColumn(entry.getValue().iterator().next()).createSecondaryIndexSearcher(entry.getValue()) );
        }
        
        return indexSearchers;
    }
    
    /**
     * Performs a search across a number of column indexes
     * TODO: add support for querying across index types
     * 
     * @param clause the index query clause
     * @param range the row range to restrict to
     * @param dataFilter the column range to restrict to
     * @return found indexed rows
     */
    public List<Row> search(IndexClause clause, AbstractBounds range, IFilter dataFilter)
    {
        List<SecondaryIndexSearcher> indexSearchers = getIndexSearchersForQuery(clause);
               
        if(indexSearchers.isEmpty())
            return Collections.emptyList();
       
        //We currently don't support searching across multiple index types
        if(indexSearchers.size() > 1)
            throw new RuntimeException("Unable to search across multiple secondary index types");
        
        
        return indexSearchers.get(0).search(clause, range, dataFilter);
    }
    
}
