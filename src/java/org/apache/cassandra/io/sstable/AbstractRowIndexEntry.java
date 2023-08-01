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

import java.io.IOException;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * The base RowIndexEntry is not stored on disk, only specifies a position in the data file
 */
public abstract class AbstractRowIndexEntry implements IMeasurableMemory
{
    public final long position;

    public AbstractRowIndexEntry(long position)
    {
        this.position = position;
    }

    /**
     * Row position in a data file
     */
    public long getPosition()
    {
        return position;
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary. Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return blockCount() > 1;
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public int blockCount()
    {
        return 0;
    }

    public abstract SSTableFormat<?, ?> getSSTableFormat();

    /**
     * Serialize this entry for key cache
     *
     * @param out the output stream for serialized entry
     */
    public abstract void serializeForCache(DataOutputPlus out) throws IOException;
}
