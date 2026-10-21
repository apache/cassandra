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

package org.apache.cassandra.db.rows;

import org.apache.cassandra.schema.ColumnMetadata;

public abstract class HeapAbstractCell<V> extends AbstractCell<V>
{
    // Careful: Adding vars here has an impact on memtable size
    protected final long timestamp;
    protected final int ttl;
    protected final int localDeletionTimeUnsignedInteger;

    protected final CellPath path;

    protected HeapAbstractCell(ColumnMetadata column, long timestamp, int ttl, int localDeletionTimeUnsignedInteger, CellPath path)
    {
        super(column);
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTimeUnsignedInteger = localDeletionTimeUnsignedInteger;
        this.path = path;
    }

    @Override
    public long timestamp()
    {
        return timestamp;
    }

    @Override
    public int ttl()
    {
        return ttl;
    }

    @Override
    protected int localDeletionTimeAsUnsignedInt()
    {
        return localDeletionTimeUnsignedInteger;
    }

    @Override
    public CellPath path()
    {
        return path;
    }
}
