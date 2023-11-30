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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * A wrapper for SSTableWriter and LifecycleTransaction to be used when
 * the writer is the only participant in the transaction and therefore
 * it can safely own the transaction.
 */
public class SSTableTxnWriter extends Transactional.AbstractTransactional implements Transactional
{
    private final LifecycleTransaction txn;
    private final SSTableMultiWriter writer;

    public SSTableTxnWriter(LifecycleTransaction txn, SSTableMultiWriter writer)
    {
        this.txn = txn;
        this.writer = writer;
    }

    public void append(UnfilteredRowIterator iterator)
    {
        writer.append(iterator);
    }

    public String getFilename()
    {
        return writer.getFilename();
    }

    public long getFilePointer()
    {
        return writer.getBytesWritten();
    }

    /**
     * Get the amount of data written to disk. Unlike {@link #getFilePointer()}, which returns the position in the
     * _uncompressed_ data, this method returns the actual file pointer position of the on disk file.
     *
     * @return the amount of data already written to disk
     */
    public long getOnDiskBytesWritten()
    {
        return writer.getOnDiskBytesWritten();
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        return writer.commit(txn.commit(accumulate));
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        return txn.abort(writer.abort(accumulate));
    }

    protected void doPrepare()
    {
        writer.prepareToCommit();
        txn.prepareToCommit();
    }

    @Override
    protected Throwable doPostCleanup(Throwable accumulate)
    {
        txn.close();
        writer.close();
        return super.doPostCleanup(accumulate);
    }

    public Collection<SSTableReader> finish(boolean openResult)
    {
        writer.setOpenResult(openResult);
        finish();
        return writer.finished();
    }

    @SuppressWarnings({"resource", "RedundantSuppression"}) // log and writer closed during doPostCleanup
    public static SSTableTxnWriter create(ColumnFamilyStore cfs, Descriptor descriptor, long keyCount, long repairedAt, TimeUUID pendingRepair, boolean isTransient, SerializationHeader header)
    {
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableMultiWriter writer = cfs.createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, header, txn);
        return new SSTableTxnWriter(txn, writer);
    }

    public static SSTableTxnWriter createRangeAware(TableMetadataRef metadata,
                                                    long keyCount,
                                                    long repairedAt,
                                                    TimeUUID pendingRepair,
                                                    boolean isTransient,
                                                    SSTableFormat<?, ?> type,
                                                    SerializationHeader header)
    {

        ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableMultiWriter writer;
        try
        {
            writer = new RangeAwareSSTableWriter(cfs, keyCount, repairedAt, pendingRepair, isTransient, type, 0, 0, txn, header);
        }
        catch (IOException e)
        {
            //We don't know the total size so this should never happen
            //as we send in 0
            throw new RuntimeException(e);
        }

        return new SSTableTxnWriter(txn, writer);
    }

    @SuppressWarnings({"resource", "RedundantSuppression"}) // log and writer closed during doPostCleanup
    public static SSTableTxnWriter create(TableMetadataRef metadata,
                                          Descriptor descriptor,
                                          long keyCount,
                                          long repairedAt,
                                          TimeUUID pendingRepair,
                                          boolean isTransient,
                                          SerializationHeader header,
                                          Collection<Index.Group> indexGroups,
                                          SSTable.Owner owner)
    {
        // if the column family store does not exist, we create a new default SSTableMultiWriter to use:
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);
        SSTableMultiWriter writer = SimpleSSTableMultiWriter.create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, null, 0, header, indexGroups, txn, owner);
        return new SSTableTxnWriter(txn, writer);
    }
}
