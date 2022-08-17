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

package org.apache.cassandra.db.memtable.pmem;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import com.intel.pmem.llpl.Transaction;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import com.intel.pmem.llpl.util.ConcurrentLongART;
import com.intel.pmem.llpl.util.LongLinkedList;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;


//Contains TableID , Cart pointer, LongLinkedList handle
// TableID - 36bytes
// Cart pointer - 8 bytes
//LongLinkedList  - 8 bytes

public class PmemTableInfo
{
    private static final int TABLE_ID_SIZE = 36;
    private static final int TABLE_ID_OFFSET = 0;
    private static final int CART_HANDLE_OFFSET = TABLE_ID_SIZE;
    private static final int SH_LIST_OFFSET = 44;
    private static final int BLOCK_SIZE = SH_LIST_OFFSET + Long.SIZE;
    private TransactionalMemoryBlock block;
    private ConcurrentLongART memtableCart;
    private TableMetadata metadata;
    //Persist serialization header block handle
    private LongLinkedList sHeaderLongLinkedList;
    //For faster access saving serializationHeader in in-memory on every table metadata change
    private List<SerializationHeader> serializationHeaderList = new ArrayList<>();

    public PmemTableInfo(TransactionalHeap heap, ConcurrentLongART memtableCart, TableMetadata metadata)
    {
        this.memtableCart = memtableCart;
        this.metadata = metadata;
        block = heap.allocateMemoryBlock(BLOCK_SIZE);
        byte[] tableUUIDBytes = metadata.id.toString().getBytes();
        block.copyFromArray(tableUUIDBytes, 0, TABLE_ID_OFFSET, tableUUIDBytes.length);
        block.setLong(CART_HANDLE_OFFSET, memtableCart.handle());
        this.sHeaderLongLinkedList = new LongLinkedList(heap);
        persistSerializationHeader();
        block.setLong(SH_LIST_OFFSET, sHeaderLongLinkedList.handle());
    }

    public PmemTableInfo(TransactionalMemoryBlock block)
    {
        this.block = block;
        this.memtableCart = ConcurrentLongART.fromHandle(block.heap(), getCartHandle());
    }

    /**
     * Recreates  PmemTableInfo from the given table handle
     *
     * @param heap        TransactionalHeap
     * @param tableHandle PmemTableInfo handle
     * @return PmemTableInfo
     */
    public static PmemTableInfo fromHandle(TransactionalHeap heap, long tableHandle)
    {
        return new PmemTableInfo(heap.memoryBlockFromHandle(tableHandle));
    }

    /**
     * @return block handle
     */
    public long handle()
    {
        return block.handle();
    }

    /**
     * Reads TableID from Persistent memory block
     *
     * @return TableId
     */
    public TableId getTableID()
    {
        int len = TABLE_ID_SIZE;
        byte[] b = new byte[len];
        block.copyToArray(TABLE_ID_OFFSET, b, 0, len);
        String str = new String(b);
        return TableId.fromUUID(UUID.fromString(str));
    }

    private long getCartHandle()
    {
        return block.getLong(TABLE_ID_SIZE);
    }

    /**
     * @return ConcurrentLongART
     */
    public ConcurrentLongART getMemtableCart()
    {
        return memtableCart;
    }

    /**
     * @return The metadata for the table
     */
    public TableMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * Updates this PmemTableInfo with given  ConcurrentLongART
     *
     * @param memtableCart ConcurrentLongART
     */
    public void updateMemtableCart(ConcurrentLongART memtableCart)
    {
        this.memtableCart = memtableCart;
        block.setLong(TABLE_ID_SIZE, memtableCart.handle());
    }

    /**
     * Frees the Persistent Memory associated with this PmemTableInfo
     */
    public void cleanUp()
    {
        Iterator<Long> itr = sHeaderLongLinkedList.iterator();
        while (itr.hasNext())
        {
            long sHeaderHandle = itr.next();
            TransactionalMemoryBlock sHeaderBlock = block.heap().memoryBlockFromHandle(sHeaderHandle);
            sHeaderBlock.free();
        }
        sHeaderLongLinkedList.clear();
        sHeaderLongLinkedList.free();
        block.free();
    }

    private long getListHandle()
    {
        return block.getLong(SH_LIST_OFFSET);
    }

    /**
     * Stores the Updated metadata and new serialization header if  applicable
     *
     * @param metadata the column family meta data
     */
    public void update(TableMetadata metadata)
    {
        this.metadata = metadata;
        if (metadata.regularAndStaticColumns().size() != getPreviousColumnCount())
        {
            persistSerializationHeader();
        }
    }

    /**
     * @return Table metadata version
     */
    public int getMetadataVersion()
    {
        return serializationHeaderList.size() - 1;
    }

    /**
     * Returns the serialization header associated with given version
     *
     * @param version Metadata version
     * @return SerializationHeader
     */
    public SerializationHeader getSerializationHeader(int version)
    {
        return serializationHeaderList.get(version);
    }

    private int getPreviousColumnCount()
    {
        int version = getMetadataVersion();
        return getSerializationHeader(version).columns().size();
    }

    /**
     * @return true if this PmemTableInfo is fully initialized
     */
    public boolean isLoaded()
    {
        return serializationHeaderList.size() > 0;
    }

    //Persist SerializationHeader on every table alter
    @SuppressWarnings({ "resource" })
    private void persistSerializationHeader()
    {
        SerializationHeader sHeader = new SerializationHeader(false,
                                                              metadata,
                                                              metadata.regularAndStaticColumns(),
                                                              EncodingStats.NO_STATS);
        TransactionalHeap heap = block.heap();
        Transaction.create(heap, () -> {
            long blockSize = SerializationHeader.serializer.serializedSize(BigFormat.latestVersion, sHeader.toComponent());
            TransactionalMemoryBlock serializationHeaderBlock = heap.allocateMemoryBlock(blockSize);
            DataOutputPlus out = new MemoryBlockDataOutputPlus(serializationHeaderBlock, 0);
            try
            {
                SerializationHeader.serializer.serialize(BigFormat.latestVersion, sHeader.toComponent(), out);
            }
            catch (IndexOutOfBoundsException | IOException e)
            {
                throw new IOError(e);
            }
            long index = sHeaderLongLinkedList.size();
            sHeaderLongLinkedList.add(index, serializationHeaderBlock.handle());
        });
        serializationHeaderList.add(sHeader);
    }

    /**
     * Reinitializes pmemtableInfo after restart
     *
     * @param metadata Column family table metadata
     */
    public void reload(TableMetadata metadata)
    {
        this.metadata = metadata;
        TransactionalHeap heap = block.heap();
        sHeaderLongLinkedList = LongLinkedList.fromHandle(heap, getListHandle());
        Iterator<Long> sHeaderLinkedList = sHeaderLongLinkedList.iterator();

        while (sHeaderLinkedList.hasNext())
        {
            long sHeaderHandle = sHeaderLinkedList.next();
            TransactionalMemoryBlock sHeaderBlock = heap.memoryBlockFromHandle(sHeaderHandle);
            DataInputPlus in = new MemoryBlockDataInputPlus(sHeaderBlock);
            try
            {
                SerializationHeader.Component component = SerializationHeader.serializer.deserialize(BigFormat.latestVersion, in);
                SerializationHeader sHeader = toSerializationHeader(component, metadata);
                serializationHeaderList.add(sHeader);
            }
            catch (IndexOutOfBoundsException | IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    // Construct SerializationHeader  from  SerializationHeader.Component
    private static SerializationHeader toSerializationHeader(SerializationHeader.Component component, TableMetadata metadata)
    {

        TableMetadata.Builder metadataBuilder = TableMetadata.builder(metadata.keyspace, metadata.name, metadata.id);
        component.getStaticColumns().entrySet().stream()
                 .forEach(entry -> {
                     ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                     metadataBuilder.addStaticColumn(ident, entry.getValue());
                 });
        component.getRegularColumns().entrySet().stream()
                 .forEach(entry -> {
                     ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                     metadataBuilder.addRegularColumn(ident, entry.getValue());
                 });
        for (ColumnMetadata columnMetadata : metadata.partitionKeyColumns())
            metadataBuilder.addPartitionKeyColumn(columnMetadata.name, columnMetadata.cellValueType());
        for (ColumnMetadata columnMetadata : metadata.clusteringColumns())
        {
            metadataBuilder.addClusteringColumn(columnMetadata.name, columnMetadata.cellValueType());
        }

        TableMetadata tableMetadata = metadataBuilder.build();
        return new SerializationHeader(false,
                                       tableMetadata,
                                       tableMetadata.regularAndStaticColumns(),
                                       EncodingStats.NO_STATS);
    }
}

