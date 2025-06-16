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

import java.util.Collection;

import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.StreamingLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Throwables;

public class SSTableTxnSingleStreamWriter implements SSTableMultiWriter
{
    private final ILifecycleTransaction txn;
    private final SSTableMultiWriter writer;
    private boolean complete = false;

    public SSTableTxnSingleStreamWriter(ILifecycleTransaction txn, SSTableMultiWriter writer)
    {
        this.txn = txn;
        this.writer = writer;
    }

    @Override
    public void append(UnfilteredRowIterator partition)
    {
        writer.append(partition);
    }

    public synchronized Collection<SSTableReader> transferOwnershipTo(StreamingLifecycleTransaction globalTxn)
    {
        failIfComplete();
        complete = true;
        writer.setOpenResult(true);
        writer.prepareToCommit();
        txn.prepareToCommit();
        globalTxn.takeOwnership(txn);
        Throwables.maybeFail(writer.commit(txn.commit(null)));
        return writer.finished();
    }

    @Override
    public synchronized Throwable abort(Throwable accumulate)
    {
        complete = true;
        return txn.abort(writer.abort(accumulate));
    }

    @Override
    public synchronized void close()
    {
        complete = true;
        writer.close();
    }

    private void failIfComplete()
    {
        if (complete)
            throw new IllegalStateException("Writer "+getFilename()+" has already completed");
    }

    @Override
    public String getFilename()
    {
        return writer.getFilename();
    }

    @Override
    public long getBytesWritten()
    {
        return writer.getBytesWritten();
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        return writer.getOnDiskBytesWritten();
    }

    @Override
    public TableId getTableId()
    {
        return writer.getTableId();
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult)
    {
        throw new UnsupportedOperationException("SSTableTxnSingleStreamWriter should be finished via transferOwnershipTo");
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        throw new UnsupportedOperationException("SSTableTxnSingleStreamWriter should be finished via transferOwnershipTo");
    }

    @Override
    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        throw new UnsupportedOperationException("SSTableTxnSingleStreamWriter should be finished via transferOwnershipTo");
    }

    @Override
    public void prepareToCommit()
    {
        throw new UnsupportedOperationException("SSTableTxnSingleStreamWriter should be finished via transferOwnershipTo");
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        throw new UnsupportedOperationException("SSTableTxnSingleStreamWriter should be finished via transferOwnershipTo");
    }

}
