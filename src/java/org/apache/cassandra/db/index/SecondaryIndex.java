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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.index.keys.KeysIndex;
import org.apache.cassandra.db.index.composites.CompositesIndex;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LocalByPartionerType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.service.StorageService;

/**
 * Abstract base class for different types of secondary indexes.
 *
 * Do not extend this directly, please pick from PerColumnSecondaryIndex or PerRowSecondaryIndex
 */
public abstract class SecondaryIndex
{
    protected static final Logger logger = LoggerFactory.getLogger(SecondaryIndex.class);

    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    /**
     * Base CF that has many indexes
     */
    protected ColumnFamilyStore baseCfs;


    /**
     * The column definitions which this index is responsible for
     */
    protected final Set<ColumnDefinition> columnDefs = Collections.newSetFromMap(new ConcurrentHashMap<ColumnDefinition,Boolean>());

    /**
     * Perform any initialization work
     */
    public abstract void init();

    /**
     * Reload an existing index following a change to its configuration,
     * or that of the indexed column(s). Differs from init() in that we expect
     * expect new resources (such as CFS for a KEYS index) to be created by
     * init() but not here
     */
    public abstract void reload();

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

    public void setIndexBuilt()
    {
        for (ColumnDefinition columnDef : columnDefs)
            SystemTable.setIndexBuilt(baseCfs.table.name, getNameForSystemTable(columnDef.name));
    }

    public void setIndexRemoved()
    {
        for (ColumnDefinition columnDef : columnDefs)
            SystemTable.setIndexRemoved(baseCfs.table.name, getNameForSystemTable(columnDef.name));
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
     */
    public abstract void forceBlockingFlush();

    /**
     * Get current amount of memory this index is consuming (in bytes)
     */
    public abstract long getLiveSize();

    /**
     * Allow access to the underlying column family store if there is one
     * @return the underlying column family store or null
     */
    public abstract ColumnFamilyStore getIndexCfs();


    /**
     * Delete all files and references to this index
     * @param columnName the indexed column to remove
     */
    public abstract void removeIndex(ByteBuffer columnName);

    /**
     * Remove the index and unregisters this index's mbean if one exists
     */
    public abstract void invalidate();

    /**
     * Truncate all the data from the current index
     *
     * @param truncatedAt The truncation timestamp, all data before that timestamp should be rejected.
     */
    public abstract void truncate(long truncatedAt);

    /**
     * Builds the index using the data in the underlying CFS
     * Blocks till it's complete
     */
    protected void buildIndexBlocking()
    {
        logger.info(String.format("Submitting index build of %s for data in %s",
                getIndexName(), StringUtils.join(baseCfs.getSSTables(), ", ")));

        Collection<SSTableReader> sstables = baseCfs.markCurrentSSTablesReferenced();
        SecondaryIndexBuilder builder = new SecondaryIndexBuilder(baseCfs,
                                                                  Collections.singleton(getIndexName()),
                                                                  new ReducingKeyIterator(sstables));
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        try
        {
            future.get();
            forceBlockingFlush();

            setIndexBuilt();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
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
                    buildIndexBlocking();
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
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

    public Set<ColumnDefinition> getColumnDefs()
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
     * Returns the decoratedKey for a column value
     * @param value column value
     * @return decorated key
     */
    public DecoratedKey getIndexKeyFor(ByteBuffer value)
    {
        // FIXME: this imply one column definition per index
        ByteBuffer name = columnDefs.iterator().next().name;
        return new DecoratedKey(new LocalToken(baseCfs.metadata.getColumnDefinition(name).getValidator(), value), value);
    }

    /**
     * Returns true if the provided column name is indexed by this secondary index.
     *
     * The default implement checks whether the name is one the columnDef name,
     * but this should be overriden but subclass if needed.
     */
    public boolean indexes(ByteBuffer name)
    {
        for (ColumnDefinition columnDef : columnDefs)
        {
            if (baseCfs.getComparator().compare(columnDef.name, name) == 0)
                return true;
        }
        return false;
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
        case COMPOSITES:
            index = new CompositesIndex();
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

    public abstract boolean validate(Column column);

    /**
     * Returns the index comparator for index backed by CFS, or null.
     *
     * Note: it would be cleaner to have this be a member method. However we need this when opening indexes
     * sstables, but by then the CFS won't be fully initiated, so the SecondaryIndex object won't be accessible.
     */
    public static AbstractType<?> getIndexComparator(CFMetaData baseMetadata, ColumnDefinition cdef)
    {
        IPartitioner rowPartitioner = StorageService.getPartitioner();
        AbstractType<?> keyComparator = (rowPartitioner instanceof OrderPreservingPartitioner || rowPartitioner instanceof ByteOrderedPartitioner)
                                      ? BytesType.instance
                                      : new LocalByPartionerType(rowPartitioner);

        switch (cdef.getIndexType())
        {
            case KEYS:
                return keyComparator;
            case COMPOSITES:
                assert baseMetadata.comparator instanceof CompositeType;
                int prefixSize;
                try
                {
                    prefixSize = Integer.parseInt(cdef.getIndexOptions().get(CompositesIndex.PREFIX_SIZE_OPTION));
                }
                catch (NumberFormatException e)
                {
                    // This shouldn't happen if validation has been done correctly
                    throw new RuntimeException(e);
                }
                List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(prefixSize + 1);
                types.add(keyComparator);
                for (int i = 0; i < prefixSize; i++)
                    types.add(((CompositeType)baseMetadata.comparator).types.get(i));
                return CompositeType.getInstance(types);
            case CUSTOM:
                return null;
        }
        throw new AssertionError();
    }
}
