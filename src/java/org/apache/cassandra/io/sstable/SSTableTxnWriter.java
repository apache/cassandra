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

import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * A wrapper for SSTableWriter and LifecycleTransaction to be used when
 * the writer is the only participant in the transaction and therefore
 * it can safely own the transaction.
 */
public class SSTableTxnWriter extends Transactional.AbstractTransactional implements Transactional
{
    private final LifecycleTransaction txn;
    private final SSTableWriter writer;

    public SSTableTxnWriter(LifecycleTransaction txn, SSTableWriter writer)
    {
        this.txn = txn;
        this.writer = writer;
    }

    public RowIndexEntry append(UnfilteredRowIterator iterator)
    {
        return writer.append(iterator);
    }

    public String getFilename()
    {
        return writer.getFilename();
    }

    public long getFilePointer()
    {
        return writer.getFilePointer();
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        return txn.commit(writer.commit(accumulate));
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

    public SSTableReader finish(boolean openResult)
    {
        writer.setOpenResult(openResult);
        finish();
        return writer.finished();
    }

    public static SSTableTxnWriter create(Descriptor descriptor, long keyCount, long repairedAt, int sstableLevel, SerializationHeader header)
    {
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, descriptor.directory);
        SSTableWriter writer = SSTableWriter.create(descriptor, keyCount, repairedAt, sstableLevel, header, txn);
        return new SSTableTxnWriter(txn, writer);
    }

    public static SSTableTxnWriter create(String filename, long keyCount, long repairedAt, int sstableLevel, SerializationHeader header)
    {
        Descriptor desc = Descriptor.fromFilename(filename);
        return create(desc, keyCount, repairedAt, sstableLevel, header);
    }

    public static SSTableTxnWriter create(String filename, long keyCount, long repairedAt, SerializationHeader header)
    {
        return create(filename, keyCount, repairedAt, 0, header);
    }
}
