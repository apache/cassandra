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

package org.apache.cassandra.db.index.keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LocalByPartionerType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public class KeysIndex extends PerColumnSecondaryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(KeysIndex.class);
    private ColumnFamilyStore indexedCfs;

    public KeysIndex() 
    {
    }
    
    public void init()
    {
        assert baseCfs != null && columnDefs != null;

        ColumnDefinition columnDef = columnDefs.iterator().next();
        CFMetaData indexedCfMetadata = CFMetaData.newIndexMetadata(baseCfs.metadata, columnDef,indexComparator());
        indexedCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.table,
                                                               indexedCfMetadata.cfName,
                                                               new LocalPartitioner(columnDef.getValidator()),
                                                               indexedCfMetadata);
    }

    public static AbstractType indexComparator()
    {
        IPartitioner rowPartitioner = StorageService.getPartitioner();
        return (rowPartitioner instanceof OrderPreservingPartitioner || rowPartitioner instanceof ByteOrderedPartitioner)
                                        ? BytesType.instance
                                        : new LocalByPartionerType(StorageService.getPartitioner());
    }

    @Override
    public void deleteColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn column)
    {
        if (column.isMarkedForDelete())
            return;
        
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ColumnFamily.create(indexedCfs.metadata);
        cfi.addTombstone(rowKey, localDeletionTime, column.timestamp());
        indexedCfs.apply(valueKey, cfi);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, cfi);
    }

    @Override
    public void insertColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn column)
    {
        ColumnFamily cfi = ColumnFamily.create(indexedCfs.metadata);
        if (column instanceof ExpiringColumn)
        {
            ExpiringColumn ec = (ExpiringColumn)column;
            cfi.addColumn(new ExpiringColumn(rowKey, ByteBufferUtil.EMPTY_BYTE_BUFFER, ec.timestamp(), ec.getTimeToLive(), ec.getLocalDeletionTime()));
        }
        else
        {
            cfi.addColumn(new Column(rowKey, ByteBufferUtil.EMPTY_BYTE_BUFFER, column.timestamp()));
        }
        if (logger.isDebugEnabled())
            logger.debug("applying index row {}:{}", valueKey, cfi);
        
        indexedCfs.apply(valueKey, cfi);
    }
    
    @Override
    public void updateColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col)
    {        
        insertColumn(valueKey, rowKey, col);        
    }

    @Override
    public void removeIndex(ByteBuffer columnName) throws IOException
    {        
        indexedCfs.unreferenceSSTables();
        indexedCfs.invalidate();
    }

    @Override
    public void forceBlockingFlush() throws IOException
    {       
        try
        {
            indexedCfs.forceBlockingFlush();
        } 
        catch (ExecutionException e)
        {
            throw new IOException(e);
        } 
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }

    @Override
    public void invalidate()
    {
        indexedCfs.invalidate();
    }

    @Override
    public ColumnFamilyStore getUnderlyingCfs()
    {
       return indexedCfs;
    }

    @Override
    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new KeysSearcher(baseCfs.indexManager, columns);
    }

    @Override
    public String getIndexName()
    {
        return indexedCfs.columnFamily;
    }

    @Override
    public void renameIndex(String newCfName) throws IOException
    {
        indexedCfs.renameSSTables(indexedCfs.columnFamily.replace(baseCfs.columnFamily, newCfName));
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        // no options used
    }
}
