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
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public abstract class AbstractSimplePerColumnSecondaryIndex extends PerColumnSecondaryIndex
{
    private ColumnFamilyStore indexCfs;

    public void init()
    {
        assert baseCfs != null && columnDefs != null && columnDefs.size() == 1;

        ColumnDefinition columnDef = columnDefs.iterator().next();
        init(columnDef);

        AbstractType indexComparator = SecondaryIndex.getIndexComparator(baseCfs.metadata, columnDef);
        CFMetaData indexedCfMetadata = CFMetaData.newIndexMetadata(baseCfs.metadata, columnDef, indexComparator);
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.table,
                                                             indexedCfMetadata.cfName,
                                                             new LocalPartitioner(columnDef.getValidator()),
                                                             indexedCfMetadata);

        // enable and initialize row cache based on parent's setting and indexed column's cardinality
        CFMetaData.Caching baseCaching = baseCfs.metadata.getCaching();
        if (baseCaching == CFMetaData.Caching.ALL || baseCaching == CFMetaData.Caching.ROWS_ONLY)
        {
            /*
             * # of index CF's key = cardinality of indexed column.
             * if # of keys stored in index CF is more than average column counts (means tall table),
             * then consider it as high cardinality.
             */
            double estimatedKeys = indexCfs.estimateKeys();
            double averageColumnCount = indexCfs.getMeanColumns();
            if (averageColumnCount > 0 && estimatedKeys / averageColumnCount > 1)
            {
                logger.debug("turning row cache on for " + indexCfs.getColumnFamilyName());
                indexCfs.metadata.caching(baseCaching);
                indexCfs.initRowCache();
            }
        }
    }

    protected abstract void init(ColumnDefinition columnDef);

    protected abstract ByteBuffer makeIndexColumnName(ByteBuffer rowKey, IColumn column);

    public void delete(ByteBuffer rowKey, IColumn column)
    {
        if (column.isMarkedForDelete())
            return;

        DecoratedKey valueKey = getIndexKeyFor(column.value());
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ColumnFamily.create(indexCfs.metadata);
        cfi.addTombstone(makeIndexColumnName(rowKey, column), localDeletionTime, column.timestamp());
        indexCfs.apply(valueKey, cfi, SecondaryIndexManager.nullUpdater);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, cfi);
    }

    public void insert(ByteBuffer rowKey, IColumn column)
    {
        DecoratedKey valueKey = getIndexKeyFor(column.value());
        ColumnFamily cfi = ColumnFamily.create(indexCfs.metadata);
        ByteBuffer name = makeIndexColumnName(rowKey, column);
        if (column instanceof ExpiringColumn)
        {
            ExpiringColumn ec = (ExpiringColumn)column;
            cfi.addColumn(new ExpiringColumn(name, ByteBufferUtil.EMPTY_BYTE_BUFFER, ec.timestamp(), ec.getTimeToLive(), ec.getLocalDeletionTime()));
        }
        else
        {
            cfi.addColumn(new Column(name, ByteBufferUtil.EMPTY_BYTE_BUFFER, column.timestamp()));
        }
        if (logger.isDebugEnabled())
            logger.debug("applying index row {} in {}", indexCfs.metadata.getKeyValidator().getString(valueKey.key), cfi);

        indexCfs.apply(valueKey, cfi, SecondaryIndexManager.nullUpdater);
    }

    public void update(ByteBuffer rowKey, IColumn col)
    {
        insert(rowKey, col);
    }

    public void removeIndex(ByteBuffer columnName)
    {
        indexCfs.invalidate();
    }

    public void forceBlockingFlush()
    {
        try
        {
            indexCfs.forceBlockingFlush();
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

    public void invalidate()
    {
        indexCfs.invalidate();
    }

    public void truncate(long truncatedAt)
    {
        indexCfs.discardSSTables(truncatedAt);
    }

    public ColumnFamilyStore getIndexCfs()
    {
       return indexCfs;
    }

    public String getIndexName()
    {
        return indexCfs.columnFamily;
    }

    public long getLiveSize()
    {
        return indexCfs.getMemtableDataSize();
    }
}
