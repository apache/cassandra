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

import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.TransactionalHeap;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.memtable.PersistentMemoryMemtable.BYTE_COMPARABLE_VERSION;

public class PmemUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator {
    private final TableMetadata tableMetadata;
    private AutoCloseableIterator<LongART.Entry> iter;
    private final ColumnFilter columnFilter;
    private final DataRange dataRange;
    private TransactionalHeap heap;
    private LongART.Entry nextEntry;
    private PmemTableInfo pmemTableInfo;

    public PmemUnfilteredPartitionIterator(TransactionalHeap heap,
                                           AutoCloseableIterator<LongART.Entry> iter,
                                           ColumnFilter columnFilter,
                                           DataRange dataRange, PmemTableInfo pmemTableInfo) {
        this.heap = heap;
        this.tableMetadata = pmemTableInfo.getMetadata();
        this.iter = iter;
        this.columnFilter = columnFilter;
        this.dataRange = dataRange;
        this.nextEntry = null;
        this.pmemTableInfo = pmemTableInfo;
    }

    @Override
    public TableMetadata metadata() {
        return tableMetadata;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public UnfilteredRowIterator next() {
        nextEntry = iter.next();
        PmemPartition pMemPartition;
        DecoratedKey dkey;
        try {
            dkey = BufferDecoratedKey.fromByteComparable(ByteComparable.fixedLength(nextEntry.getKey()),
                    BYTE_COMPARABLE_VERSION, tableMetadata.partitioner);
            pMemPartition = new PmemPartition(heap, dkey, null, UpdateTransaction.NO_OP, pmemTableInfo);
            pMemPartition.load(heap, nextEntry.getValue(), EncodingStats.NO_STATS);
        } catch (Exception e) {
            closeCartIterator();
            throw e;
        }
        ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(dkey);
        return filter.getUnfilteredRowIterator(columnFilter, pMemPartition);
    }


    @Override
    public void close() {
        if (iter != null) {
            closeCartIterator();
        }
    }

    //Need to do this for lock on cart to be released
    private void closeCartIterator() {
        try {
            iter.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }
}
