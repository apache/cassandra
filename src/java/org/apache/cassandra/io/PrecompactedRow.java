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


import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.PageCacheInformer;
import org.apache.cassandra.utils.Pair;

/**
 * PrecompactedRow merges its rows in its constructor in memory.
 */
public class PrecompactedRow extends AbstractCompactedRow
{
    private static Logger logger = LoggerFactory.getLogger(PrecompactedRow.class);

    private final DataOutputBuffer buffer;
    private int columnCount = 0;

    public PrecompactedRow(DecoratedKey key, DataOutputBuffer buffer)
    {
        super(key);
        this.buffer = buffer;
    }

    public PrecompactedRow(ColumnFamilyStore cfStore, List<SSTableIdentityIterator> rows, boolean major, int gcBefore, boolean forceDeserialize)
    {
        super(rows.get(0).getKey());
        buffer = new DataOutputBuffer();

        Set<SSTable> sstables = new HashSet<SSTable>();
        for (SSTableIdentityIterator row : rows)
        {
            sstables.add(row.sstable);
        }
        boolean shouldPurge = major || !cfStore.isKeyInRemainingSSTables(key, sstables);

        if (rows.size() > 1 || shouldPurge || !rows.get(0).sstable.descriptor.isLatestVersion || forceDeserialize)
        {
            ColumnFamily cf = null;
            for (SSTableIdentityIterator row : rows)
            {
                ColumnFamily thisCF;
                try
                {
                    thisCF = row.getColumnFamilyWithColumns();
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

                if (row.hasColumnsInPageCache())
                    this.hasColumnsInPageCache = true;
            }

            ColumnFamily cfPurged = shouldPurge ? ColumnFamilyStore.removeDeleted(cf, gcBefore) : cf;
            if (cfPurged == null)
                return;
            columnCount = ColumnFamily.serializer().serializeWithIndexes(cfPurged, buffer);
        }
        else
        {
            assert rows.size() == 1;
            try
            {
                rows.get(0).echoData(buffer);
                columnCount = rows.get(0).columnCount;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    public void write(PageCacheInformer out) throws IOException
    {
        out.writeLong(buffer.getLength());

        List<Pair<Integer, Integer>> pageCacheMarkers = buffer.getPageCacheMarkers();

        if(pageCacheMarkers == null)
        {
             out.write(buffer.getData(), 0, buffer.getLength());
             return;
        }

        // Step through each page cache window and inform the
        // output writer to respect these...
        long startingPosition = out.getCurrentPosition();
        int bufferOffset = 0;
        for (Pair<Integer,Integer> window : pageCacheMarkers)
        {
            // write out any data before the window
            if (window.left > (out.getCurrentPosition() - startingPosition))
            {
                out.write(buffer.getData(), bufferOffset, window.left - bufferOffset);
                bufferOffset = window.left;
            }

            long startingAt = out.getCurrentPosition();

            assert (bufferOffset + window.right) <= buffer.getLength() : ""+(bufferOffset + window.right)+" > "+buffer.getLength();

            out.write(buffer.getData(), bufferOffset, window.right);
            out.keepCacheWindow(startingAt);

            bufferOffset += window.right;
        }

        // Write everything else
        if (bufferOffset < buffer.getLength())
        {
            out.write(buffer.getData(), bufferOffset, buffer.getLength() - bufferOffset);
        }
    }

    public void update(MessageDigest digest)
    {
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    public boolean isEmpty()
    {
        return buffer.getLength() == 0;
    }

    public int columnCount()
    {
        return columnCount;
    }
}
