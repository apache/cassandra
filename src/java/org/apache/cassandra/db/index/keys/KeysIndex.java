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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.LocalByPartionerType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public class KeysIndex extends SecondaryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(KeysIndex.class);
    private final ColumnFamilyStore indexedCfs;
    private final ConcurrentSkipListSet<Memtable> fullMemtables;

    public KeysIndex(ColumnFamilyStore baseCfs, ColumnDefinition cdef)
    {
        super(baseCfs, cdef);

        fullMemtables = new ConcurrentSkipListSet<Memtable>();

        IPartitioner<?> rowPartitioner = StorageService.getPartitioner();
        AbstractType<?> columnComparator = (rowPartitioner instanceof OrderPreservingPartitioner || rowPartitioner instanceof ByteOrderedPartitioner)
                                            ? BytesType.instance
                                            : new LocalByPartionerType(StorageService.getPartitioner());

        final CFMetaData indexedCfMetadata = CFMetaData.newIndexMetadata(baseCfs.metadata, cdef, columnComparator);

        indexedCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.table,
                                                               indexedCfMetadata.cfName,
                                                               new LocalPartitioner(cdef.getValidator()),
                                                               indexedCfMetadata);
    }

    public IndexType type()
    {
        return IndexType.KEYS;
    }

    @Override
    public void deleteColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn column)
    {
        if (column.isMarkedForDelete())
            return;
        
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ColumnFamily.create(indexedCfs.metadata);
        cfi.addTombstone(rowKey, localDeletionTime, column.timestamp());
        Memtable fullMemtable = indexedCfs.apply(valueKey, cfi);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, cfi);
        if (fullMemtable != null)
            fullMemtables.add(fullMemtable);
            
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
        
        Memtable fullMemtable = indexedCfs.apply(valueKey, cfi);
        
        if (fullMemtable != null)
            fullMemtables.add(fullMemtable);
    }
    
    @Override
    public void updateColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col)
    {        
        insertColumn(valueKey, rowKey, col);        
    }

    @Override
    public void commitRow(ByteBuffer rowKey)
    {
       //nothing required in this impl since indexes are per column 
    }

    @Override
    public void maybeFlush()
    {
        Iterator<Memtable> iterator = fullMemtables.iterator();
        while(iterator.hasNext())
        {           
            Memtable memtable = iterator.next();
            
            if(memtable == null)
                continue;
            
            memtable.cfs.maybeSwitchMemtable(memtable, false);
            iterator.remove();
        }      
    }

    @Override
    public void removeIndex()
    {
        indexedCfs.removeAllSSTables();
        indexedCfs.unregisterMBean();
    }

    @Override
    public void forceBlockingFlush() throws ExecutionException, InterruptedException
    {
        indexedCfs.forceBlockingFlush();
    }

    @Override
    public void unregisterMbean()
    {
        indexedCfs.unregisterMBean();        
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
}
