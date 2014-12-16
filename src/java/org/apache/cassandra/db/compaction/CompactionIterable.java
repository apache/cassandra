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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class CompactionIterable extends AbstractCompactionIterable
{
    private static final Comparator<OnDiskAtomIterator> comparator = new Comparator<OnDiskAtomIterator>()
    {
        public int compare(OnDiskAtomIterator i1, OnDiskAtomIterator i2)
        {
            return i1.getKey().compareTo(i2.getKey());
        }
    };

    public CompactionIterable(OperationType type, List<ISSTableScanner> scanners, CompactionController controller)
    {
        super(controller, type, scanners);
    }

    public CloseableIterator<AbstractCompactedRow> iterator()
    {
        return MergeIterator.get(scanners, comparator, new Reducer());
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    protected class Reducer extends MergeIterator.Reducer<OnDiskAtomIterator, AbstractCompactedRow>
    {
        protected final List<OnDiskAtomIterator> rows = new ArrayList<>();

        public void reduce(OnDiskAtomIterator current)
        {
            rows.add(current);
        }

        protected AbstractCompactedRow getReduced()
        {
            assert !rows.isEmpty();

            CompactionIterable.this.updateCounterFor(rows.size());
            try
            {
                // create a new container for rows, since we're going to clear ours for the next one,
                // and the AbstractCompactionRow code should be able to assume that the collection it receives
                // won't be pulled out from under it.
                return new LazilyCompactedRow(controller, ImmutableList.copyOf(rows));
            }
            finally
            {
                rows.clear();
                long n = 0;
                for (ISSTableScanner scanner : scanners)
                    n += scanner.getCurrentPosition();
                bytesRead = n;
            }
        }
    }
}
