package org.apache.cassandra.io;
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


import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.log4j.Logger;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;

public class CompactionIterator extends ReducingIterator<IteratingRow, CompactionIterator.CompactedRow> implements Closeable
{
    private static Logger logger = Logger.getLogger(CompactionIterator.class);

    private final List<IteratingRow> rows = new ArrayList<IteratingRow>();
    private final int gcBefore;

    @SuppressWarnings("unchecked")
    public CompactionIterator(Iterable<SSTableReader> sstables, int gcBefore) throws IOException
    {
        super(getCollatingIterator(sstables));
        this.gcBefore = gcBefore;
    }

    @SuppressWarnings("unchecked")
    private static CollatingIterator getCollatingIterator(Iterable<SSTableReader> sstables) throws IOException
    {
        // CollatingIterator has a bug that causes NPE when you try to use default comparator. :(
        CollatingIterator iter = new CollatingIterator(new Comparator()
        {
            public int compare(Object o1, Object o2)
            {
                return ((Comparable)o1).compareTo(o2);
            }
        });
        for (SSTableReader sstable : sstables)
        {
            iter.addIterator(sstable.getScanner());
        }
        return iter;
    }

    @Override
    protected boolean isEqual(IteratingRow o1, IteratingRow o2)
    {
        return o1.getKey().equals(o2.getKey());
    }

    public void reduce(IteratingRow current)
    {
        rows.add(current);
    }

    protected CompactedRow getReduced()
    {
        assert rows.size() > 0;
        DataOutputBuffer buffer = new DataOutputBuffer();
        DecoratedKey key = rows.get(0).getKey();

        ColumnFamily cf = null;
        for (IteratingRow row : rows)
        {
            ColumnFamily thisCF;
            try
            {
                thisCF = row.getColumnFamily();
            }
            catch (IOException e)
            {
                logger.error("Skipping row " + key + " in " + row.getPath(), e);
                continue;
            }
            if (cf == null)
            {
                cf = thisCF;
            }
            else
            {
                cf.addAll(thisCF);
            }
        }
        rows.clear();

        ColumnFamily cfPurged = ColumnFamilyStore.removeDeleted(cf, gcBefore);
        if (cfPurged == null)
            return null;
        ColumnFamily.serializer().serializeWithIndexes(cfPurged, buffer);

        return new CompactedRow(key, buffer);
    }

    public void close() throws IOException
    {
        for (Object o : ((CollatingIterator)source).getIterators())
        {
            ((SSTableScanner)o).close();
        }
    }

    public static class CompactedRow
    {
        public final DecoratedKey key;
        public final DataOutputBuffer buffer;

        public CompactedRow(DecoratedKey key, DataOutputBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }
    }
}
