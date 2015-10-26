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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Caller must acquire and release references to the sstables used here.
 */
public class ReducingKeyIterator implements CloseableIterator<DecoratedKey>
{
    private final ArrayList<KeyIterator> iters;
    private IMergeIterator<DecoratedKey,DecoratedKey> mi;

    public ReducingKeyIterator(Collection<SSTableReader> sstables)
    {
        iters = new ArrayList<>(sstables.size());
        for (SSTableReader sstable : sstables)
            iters.add(new KeyIterator(sstable.descriptor, sstable.metadata));
    }

    private void maybeInit()
    {
        if (mi == null)
        {
            mi = MergeIterator.get(iters, DecoratedKey.comparator, new MergeIterator.Reducer<DecoratedKey,DecoratedKey>()
            {
                DecoratedKey reduced = null;

                @Override
                public boolean trivialReduceIsTrivial()
                {
                    return true;
                }

                public void reduce(int idx, DecoratedKey current)
                {
                    reduced = current;
                }

                protected DecoratedKey getReduced()
                {
                    return reduced;
                }
            });
        }
    }

    public void close()
    {
        if (mi != null)
            mi.close();
    }

    public long getTotalBytes()
    {
        maybeInit();

        long m = 0;
        for (Iterator<DecoratedKey> iter : mi.iterators())
        {
            m += ((KeyIterator) iter).getTotalBytes();
        }
        return m;
    }

    public long getBytesRead()
    {
        maybeInit();

        long m = 0;
        for (Iterator<DecoratedKey> iter : mi.iterators())
        {
            m += ((KeyIterator) iter).getBytesRead();
        }
        return m;
    }

    public boolean hasNext()
    {
        maybeInit();
        return mi.hasNext();
    }

    public DecoratedKey next()
    {
        maybeInit();
        return mi.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
