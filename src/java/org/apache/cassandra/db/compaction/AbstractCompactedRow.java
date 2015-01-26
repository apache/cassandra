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

import java.io.Closeable;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.util.SequentialWriter;

/**
 * a CompactedRow is an object that takes a bunch of rows (keys + columnfamilies)
 * and can write a compacted version of those rows to an output stream.  It does
 * NOT necessarily require creating a merged CF object in memory.
 */
public abstract class AbstractCompactedRow implements Closeable
{
    public final DecoratedKey key;

    public AbstractCompactedRow(DecoratedKey key)
    {
        this.key = key;
    }

    /**
     * write the row (size + column index + filter + column data, but NOT row key) to @param out.
     *
     * write() may change internal state; it is NOT valid to call write() or update() a second time.
     *
     * @return index information for the written row, or null if the compaction resulted in only expired tombstones.
     */
    public abstract RowIndexEntry write(long currentPosition, SequentialWriter out) throws IOException;

    /**
     * update @param digest with the data bytes of the row (not including row key or row size).
     * May be called even if empty.
     *
     * update() may change internal state; it is NOT valid to call write() or update() a second time.
     */
    public abstract void update(MessageDigest digest);

    /**
     * @return aggregate information about the columns in this row.  Some fields may
     * contain default values if computing them value would require extra effort we're not willing to make.
     */
    public abstract ColumnStats columnStats();
}
