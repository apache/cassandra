package org.apache.cassandra.io.sstable;
/*
 *
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
 *
 */


import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Two approaches to building an IndexSummary:
 * 1. Call maybeAddEntry with every potential index entry
 * 2. Call shouldAddEntry, [addEntry,] incrementRowid
 */
public class IndexSummary
{
    private ArrayList<Long> positions;
    private ArrayList<DecoratedKey<?>> keys;
    private long keysWritten = 0;

    public IndexSummary(long expectedKeys)
    {
        long expectedEntries = expectedKeys / DatabaseDescriptor.getIndexInterval();
        if (expectedEntries > Integer.MAX_VALUE)
            // TODO: that's a _lot_ of keys, or a very low interval
            throw new RuntimeException("Cannot use index_interval of " + DatabaseDescriptor.getIndexInterval() + " with " + expectedKeys + " (expected) keys.");
        positions = new ArrayList<Long>((int)expectedEntries);
        keys = new ArrayList<DecoratedKey<?>>((int)expectedEntries);
    }

    public void incrementRowid()
    {
        keysWritten++;
    }

    public boolean shouldAddEntry()
    {
        return keysWritten % DatabaseDescriptor.getIndexInterval() == 0;
    }

    public void addEntry(DecoratedKey<?> key, long indexPosition)
    {
        keys.add(SSTable.getMinimalKey(key));
        positions.add(indexPosition);
    }

    public void maybeAddEntry(DecoratedKey<?> decoratedKey, long indexPosition)
    {
        if (shouldAddEntry())
            addEntry(decoratedKey, indexPosition);
        incrementRowid();
    }

    public List<DecoratedKey<?>> getKeys()
    {
        return keys;
    }

    public long getPosition(int index)
    {
        return positions.get(index);
    }

    public void complete()
    {
        keys.trimToSize();
        positions.trimToSize();
    }
}
