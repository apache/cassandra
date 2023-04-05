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

package org.apache.cassandra.db.lifecycle;

import java.util.Collection;
import java.util.Set;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class WrappedLifecycleTransaction implements ILifecycleTransaction
{

    final ILifecycleTransaction delegate;
    public WrappedLifecycleTransaction(ILifecycleTransaction delegate)
    {
        this.delegate = delegate;
    }

    public void checkpoint()
    {
        delegate.checkpoint();
    }

    public void update(SSTableReader reader, boolean original)
    {
        delegate.update(reader, original);
    }

    public void update(Collection<SSTableReader> readers, boolean original)
    {
        delegate.update(readers, original);
    }

    public SSTableReader current(SSTableReader reader)
    {
        return delegate.current(reader);
    }

    public void obsolete(SSTableReader reader)
    {
        delegate.obsolete(reader);
    }

    public void obsoleteOriginals()
    {
        delegate.obsoleteOriginals();
    }

    public Set<SSTableReader> originals()
    {
        return delegate.originals();
    }

    public boolean isObsolete(SSTableReader reader)
    {
        return delegate.isObsolete(reader);
    }

    public Throwable commit(Throwable accumulate)
    {
        return delegate.commit(accumulate);
    }

    public Throwable abort(Throwable accumulate)
    {
        return delegate.abort(accumulate);
    }

    public void prepareToCommit()
    {
        delegate.prepareToCommit();
    }

    public void close()
    {
        delegate.close();
    }

    public void trackNew(SSTable table)
    {
        delegate.trackNew(table);
    }

    public void untrackNew(SSTable table)
    {
        delegate.untrackNew(table);
    }

    public OperationType opType()
    {
        return delegate.opType();
    }

    public boolean isOffline()
    {
        return delegate.isOffline();
    }
}
