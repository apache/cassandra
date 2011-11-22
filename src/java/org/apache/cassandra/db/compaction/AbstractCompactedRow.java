package org.apache.cassandra.db.compaction;
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
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.cassandra.db.DecoratedKey;

/**
 * a CompactedRow is an object that takes a bunch of rows (keys + columnfamilies)
 * and can write a compacted version of those rows to an output stream.  It does
 * NOT necessarily require creating a merged CF object in memory.
 */
public abstract class AbstractCompactedRow
{
    public final DecoratedKey<?> key;

    public AbstractCompactedRow(DecoratedKey<?> key)
    {
        this.key = key;
    }

    /**
     * write the row (size + column index + filter + column data, but NOT row key) to @param out.
     * It is an error to call this if isEmpty is false.  (Because the key is appended first,
     * so we'd have an incomplete row written.)
     *
     * write() may change internal state; it is NOT valid to call write() or update() a second time.
     */
    public abstract long write(DataOutput out) throws IOException;

    /**
     * update @param digest with the data bytes of the row (not including row key or row size).
     * May be called even if empty.
     *
     * update() may change internal state; it is NOT valid to call write() or update() a second time.
     */
    public abstract void update(MessageDigest digest);

    /**
     * @return true if there are no columns in the row AND there are no row-level tombstones to be preserved
     */
    public abstract boolean isEmpty();

    /**
     * @return the number of columns in the row
     */
    public abstract int columnCount();

    /**
     * @return the max column timestamp in the row or Long.MIN_VALUE if
     * computing this value would require extra effort we're not willing to
     * make.
     */
    public abstract long maxTimestamp();
}
