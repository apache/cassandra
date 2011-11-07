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
import java.util.concurrent.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for different types of secondary indexes.
 * 
 * Do not extend this directly, please pick from PerColumnSecondaryIndex or PerRowSecondaryIndex
 */
public abstract class SecondaryIndex
{
    
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndex.class);
    
    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    /**
     * Base CF that has many indexes
     */
    protected ColumnFamilyStore baseCfs;
    
    
    /**
     * The column definitions which this index is responsible for
     */
    protected Set<ColumnDefinition> columnDefs = Collections.newSetFromMap(new ConcurrentHashMap<ColumnDefinition,Boolean>());
    
    /**
     * Perform any initialization work
     */
    public abstract void init();
    
    /**
     * Validates the index_options passed in the ColumnDef
     * @throws ConfigurationException
     */
    public abstract void validateOptions() throws ConfigurationException;

    
    /**
     * @return The name of the index
     */
    abstract public String getIndexName();
   
    
    /**
     * Return the unique name for this index and column
     * to be stored in the SystemTable that tracks if each column is built
     * 
     * @param columnName the name of the column
     * @return the unique name
     */
    abstract public String getNameForSystemTable(ByteBuffer columnName);
      
    /**
     * Checks if the index for specified column is fully built
     * 
     * @param columnName the column
     * @return true if the index is fully built
     */
    public boolean isIndexBuilt(ByteBuffer columnName)
    {
        return SystemTable.isIndexBuilt(baseCfs.table.name, getNameForSystemTable(columnName));
    }
    
    /**
     * Called at query time
     * Creates a implementation specific searcher instance for this index type
     * @param columns the list of columns which belong to this index type
     * @return the secondary index search impl
     */
    protected abstract SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns);
        
    /**
     * Forces this indexes in memory data to disk
     * @throws IOException
     */
    public abstract void forceBlockingFlush() throws IOException;

    /**
     * Allow access to the underlying column family store if there is one
     * @return the underlying column family store or null
     */
    public abstract ColumnFamilyStore getIndexCfs();
   
    
    /**
     * Delete all files and references to this index
     * @param columnName the indexed column to remove
     */
    public abstract void removeIndex(ByteBuffer columnName) throws IOException;

    /**
     * Remove the index and unregisters this index's mbean if one exists
     */
    public abstract void invalidate();
    
    
    /**
     * Builds the index using the data in the underlying CFS
     * Blocks till it's complete
     */
    protected void buildIndexBlocking() throws IOException
    {
        logger.info(String.format("Submitting index build of %s for data in %s",
                getIndexName(), StringUtils.join(baseCfs.getSSTables(), ", ")));

        SortedSet<ByteBuffer> columnNames = new TreeSet<ByteBuffer>();
        
        for (ColumnDefinition cdef : columnDefs)
            columnNames.add(cdef.name);

        Collection<SSTableReader> sstables = baseCfs.markCurrentSSTablesReferenced();
        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs,
                                                                  columnNames,
                                                                  new ReducingKeyIterator(sstables));
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        try
        {
            future.get();
            forceBlockingFlush();
            
            // Mark all indexed columns as built
            if (this instanceof PerRowSecondaryIndex)
            {
                for (ByteBuffer columnName : columnNames)
                    SystemTable.setIndexBuilt(baseCfs.table.name, getIndexName()+ByteBufferUtil.string(columnName));
            }
            else
            {
                SystemTable.setIndexBuilt(baseCfs.table.name, getIndexName());
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new IOException(e);
        }
        finally
        {
            SSTableReader.releaseReferences(sstables);
        }
        logger.info("Index build of " + getIndexName() + " complete");
    }

    
    /**
     * Builds the index using the data in the underlying CF, non blocking
     * 
     * 
     * @return A future object which the caller can block on (optional)
     */
    public Future<?> buildIndexAsync()
    {
        // if we're just linking in the index to indexedColumns on an already-built index post-restart, we're done
        boolean allAreBuilt = true;
        for (ColumnDefinition cdef : columnDefs)
        {
            if (!SystemTable.isIndexBuilt(baseCfs.table.name, getNameForSystemTable(cdef.name)))
            {
                allAreBuilt = false;
                break;
            }
        }
        
        if (allAreBuilt)
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
                
                try
                {
                    buildIndexBlocking();
                } 
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        FutureTask<?> f = new FutureTask<Object>(runnable, null);
       
        new Thread(f, "Creating index: " + getIndexName()).start();
        return f;
    }
    
    public ColumnFamilyStore getBaseCfs()
    {
        return baseCfs;
    }

    private void setBaseCfs(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }

    Set<ColumnDefinition> getColumnDefs()
    {
        return columnDefs;
    }

    void addColumnDef(ColumnDefinition columnDef)
    {
       columnDefs.add(columnDef);
    }
    
    void removeColumnDef(ByteBuffer name)
    {
        Iterator<ColumnDefinition> it = columnDefs.iterator();
        while (it.hasNext())
        {
            if (it.next().name.equals(name))
                it.remove();
        }
    }
    
    /**
     * This is the primary way to create a secondary index instance for a CF column.
     * It will validate the index_options before initializing.
     * 
     * @param baseCfs the source of data for the Index
     * @param cdef the meta information about this column (index_type, index_options, name, etc...)
     *
     * @return The secondary index instance for this column
     * @throws ConfigurationException
     */
    public static SecondaryIndex createInstance(ColumnFamilyStore baseCfs, ColumnDefinition cdef) throws ConfigurationException
    {
        SecondaryIndex index;
        
        switch (cdef.getIndexType())
        {
        case KEYS: 
            index = new KeysIndex();
            break;
        case CUSTOM:           
            assert cdef.getIndexOptions() != null;
            String class_name = cdef.getIndexOptions().get(CUSTOM_INDEX_OPTION_NAME);
            assert class_name != null;
            try
            {
                index = (SecondaryIndex) Class.forName(class_name).newInstance();
            } 
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }            
            break;
            default:
                throw new RuntimeException("Unknown index type: " + cdef.getIndexName());
        }
        
        index.addColumnDef(cdef);
        index.validateOptions();
        index.setBaseCfs(baseCfs);
        
        return index;
    }
}
