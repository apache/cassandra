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


import java.io.DataOutput;
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
import org.apache.cassandra.db.ColumnIndexer;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;

/**
 * PrecompactedRow merges its rows in its constructor in memory.
 */
public class PrecompactedRow extends AbstractCompactedRow
{
    private static Logger logger = LoggerFactory.getLogger(PrecompactedRow.class);

    private final ColumnFamily compactedCf;

    public PrecompactedRow(DecoratedKey key, ColumnFamily compacted)
    {
        super(key);
        this.compactedCf = compacted;
    }

    public PrecompactedRow(ColumnFamilyStore cfStore, List<SSTableIdentityIterator> rows, boolean major, int gcBefore, boolean forceDeserialize)
    {
        super(rows.get(0).getKey());

        Set<SSTable> sstables = new HashSet<SSTable>();
        for (SSTableIdentityIterator row : rows)
        {
            sstables.add(row.sstable);
        }
        boolean shouldPurge = major || !cfStore.isKeyInRemainingSSTables(key, sstables);

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
        }
        compactedCf = shouldPurge ? ColumnFamilyStore.removeDeleted(cf, gcBefore) : cf;
        if (compactedCf != null && compactedCf.metadata().getDefaultValidator().isCommutative())
        {
            CounterColumn.removeOldShards(compactedCf, gcBefore);
        }
    }

    public void write(DataOutput out) throws IOException
    {
        if (compactedCf != null)
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            DataOutputBuffer headerBuffer = new DataOutputBuffer();
            ColumnIndexer.serialize(compactedCf, headerBuffer);
            ColumnFamily.serializer().serializeForSSTable(compactedCf, buffer);
            out.writeLong(headerBuffer.getLength() + buffer.getLength());
            out.write(headerBuffer.getData(), 0, headerBuffer.getLength());
            out.write(buffer.getData(), 0, buffer.getLength());
        }
    }

    public void update(MessageDigest digest)
    {
        if (compactedCf != null)
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            try
            {
                ColumnFamily.serializer().serializeCFInfo(compactedCf, buffer);
                buffer.writeInt(compactedCf.getColumnCount());
                digest.update(buffer.getData(), 0, buffer.getLength());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            compactedCf.updateDigest(digest);
        }
    }

    public boolean isEmpty()
    {
        return compactedCf == null || compactedCf.getColumnCount() == 0;
    }

    public int columnCount()
    {
        return compactedCf == null ? 0 : compactedCf.getColumnCount();
    }
}
