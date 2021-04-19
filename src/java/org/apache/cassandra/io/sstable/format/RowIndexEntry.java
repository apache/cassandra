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

package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * The base RowIndexEntry is not stored on disk, only specifies a position in the data file
 */
public class RowIndexEntry implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0));

    /**
     * Row position in a data file
     */
    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary. Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return columnsIndexCount() > 1;
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public int columnsIndexCount()
    {
        return 0;
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }
}
