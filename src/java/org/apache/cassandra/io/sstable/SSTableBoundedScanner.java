/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.Pair;

/**
 * A SSTableScanner that only reads key in a given range (for validation compaction).
 */
public class SSTableBoundedScanner extends SSTableScanner
{
    private final Iterator<Pair<Long, Long>> rangeIterator;
    private Pair<Long, Long> currentRange;

    SSTableBoundedScanner(SSTableReader sstable, boolean skipCache, Range range)
    {
        super(sstable, skipCache);
        this.rangeIterator = sstable.getPositionsForRanges(Collections.singletonList(range)).iterator();
        if (rangeIterator.hasNext())
        {
            currentRange = rangeIterator.next();
            try
            {
                file.seek(currentRange.left);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            exhausted = true;
        }
    }

    @Override
    public boolean hasNext()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IColumnIterator[0]).iterator() : new BoundedKeyScanningIterator();
        return iterator.hasNext();
    }

    @Override
    public IColumnIterator next()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IColumnIterator[0]).iterator() : new BoundedKeyScanningIterator();
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
