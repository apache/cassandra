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
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for different types of secondary indexes.
 * 
 * This abstraction assumes there is one column per index instance
 */
public abstract class SecondaryIndex
{
    
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndex.class);
    
    /**
     * Base CF that has many indexes
     */
    protected final ColumnFamilyStore baseCfs;
    
    /**
     * The column definition which this index is responsible for
     */
    protected final ColumnDefinition columnDef;

    
    public SecondaryIndex(ColumnFamilyStore baseCfs, ColumnDefinition columnDef)
    {
        this.baseCfs = baseCfs;
        this.columnDef = columnDef;
    }
    
    /**
     * @return The type of index.
     */
    public abstract IndexType type();

    
    /**
     * @return The name of the index
     */
    abstract public String getIndexName();
   
        
    /**
     * Delete a column from the index
     * 
     * @param valueKey the column value which is used as the index key
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void deleteColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col);
    
    /**
     * insert a column to the index
     * 
     * @param valueKey the column value which is used as the index key
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void insertColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col);
    
    /**
     * update a column from the index
     * 
     * @param valueKey the column value which is used as the index key
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void updateColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col);
    
    
    /**
     * Called after a number of insert/delete calls
     * this is required for indexes that keep many columns per row
     * depends on the underlying impl
     * @param rowKey the row identifier that was completed
     */
    public abstract void commitRow(ByteBuffer rowKey);
    
    
    /**
     * Called at query time
     * Creates a implementation specific searcher instance for this index type
     * @param columns the list of columns which belong to this index type
     * @return the secondary index search impl
     */
    protected abstract SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns);
    
    /**
     * Depending on the underlying impl, we might need to flush data to disk
     */
    public abstract void maybeFlush();
    
    /**
     * Forces this indexes in memory data to disk
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public abstract void forceBlockingFlush() throws ExecutionException, InterruptedException;

    /**
     * Allow access to the underlying column family store if there is one
     * @return the underlying column family store or null
     */
    public abstract ColumnFamilyStore getUnderlyingCfs();
    
    
    /**
     * Check if index is already built for current store
     * @return true if built, false otherwise
     */
    public boolean isIndexBuilt()
    {
        return SystemTable.isIndexBuilt(baseCfs.table.name, getIndexName());
    }
    
    /**
     * Delete all files and references to this index
     */
    public abstract void removeIndex();
    
    /**
     * Renames the underlying index files to reflect the new CF name
     * @param newCfName new column family name.
     * @throws IOException on any I/O error.
     */
    public abstract void renameIndex(String newCfName) throws IOException;
    
    /**
     * Unregisters this index's mbean if one exists
     */
    public abstract void unregisterMbean();
    
    
    /**
     * Builds the index using the data in the underlying CFS
     * Blocks till it's complete
     */
    protected void buildIndexBlocking()
    {
        logger.info(String.format("Submitting index build of %s for data in %s",
                baseCfs.metadata.comparator.getString(columnDef.name), StringUtils.join(baseCfs.getSSTables(), ", ")));

        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs,
                                                                  new TreeSet<ByteBuffer>(Collections.singleton(columnDef.name)),
                                                                  new ReducingKeyIterator(baseCfs.getSSTables()));

        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        try
        {
            future.get();
            forceBlockingFlush();
            SystemTable.setIndexBuilt(baseCfs.table.name, getIndexName());
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        logger.info("Index build of " + baseCfs.metadata.comparator.getString(columnDef.name) + " complete");
    }

    
    /**
     * Builds the index using the data in the underlying CF, non blocking
     * @return A future object which the caller can block on (optional)
     */
    public Future<?> buildIndexAsync()
    {
        // if we're just linking in the index to indexedColumns on an already-built index post-restart, we're done
        if (isIndexBuilt())
            return null;

        // build it asynchronously; addIndex gets called by CFS open and schema update, neither of which
        // we want to block for a long period.  (actual build is serialized on CompactionManager.)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    baseCfs.forceBlockingFlush();
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                
                buildIndexBlocking();
            }
        };
        FutureTask<?> f = new FutureTask<Object>(runnable, null);
       
        new Thread(f, "Creating index: " + columnDef.getIndexName()).start();
        return f;
    }    
     
}
