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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

/**
 * A SSTableScanner that only reads key in a given range (for validation compaction).
 */
public class SSTableBoundedScanner extends SSTableScanner
{
    private final Iterator<Pair<Long, Long>> rangeIterator;
    private Pair<Long, Long> currentRange;

    SSTableBoundedScanner(SSTableReader sstable, boolean skipCache, Range<Token> range)
    {
        super(sstable, skipCache);
        this.rangeIterator = sstable.getPositionsForRanges(Collections.singletonList(range)).iterator();
        if (rangeIterator.hasNext())
        {
            currentRange = rangeIterator.next();
            dfile.seek(currentRange.left);
        }
        else
        {
            exhausted = true;
        }
    }

    /*
     * This shouldn't be used with a bounded scanner as it could put the
     * bounded scanner outside it's range.
     */
    @Override
    public void seekTo(RowPosition seekKey)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new OnDiskAtomIterator[0]).iterator() : new BoundedKeyScanningIterator();
        return iterator.hasNext();
    }

    @Override
    public OnDiskAtomIterator next()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new OnDiskAtomIterator[0]).iterator() : new BoundedKeyScanningIterator();
        return iterator.next();
    }

    protected class BoundedKeyScanningIterator extends KeyScanningIterator
    {
        @Override
        public boolean hasNext()
        {
            if (!super.hasNext())
                return false;

            if (finishedAt < currentRange.right)
                return true;

            if (rangeIterator.hasNext())
            {
                currentRange = rangeIterator.next();
                finishedAt = currentRange.left; // next() will seek for us
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
