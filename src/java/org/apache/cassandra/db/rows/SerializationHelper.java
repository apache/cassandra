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

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SerializationHelper
{
    /**
     * Flag affecting deserialization behavior (this only affect counters in practice).
     *  - LOCAL: for deserialization of local data (Expired columns are
     *      converted to tombstones (to gain disk space)).
     *  - FROM_REMOTE: for deserialization of data received from remote hosts
     *      (Expired columns are converted to tombstone and counters have
     *      their delta cleared)
     *  - PRESERVE_SIZE: used when no transformation must be performed, i.e,
     *      when we must ensure that deserializing and reserializing the
     *      result yield the exact same bytes. Streaming uses this.
     */
    public static enum Flag
    {
        LOCAL, FROM_REMOTE, PRESERVE_SIZE;
    }

    private final Flag flag;
    public final int version;

    private final ReusableLivenessInfo livenessInfo = new ReusableLivenessInfo();

    // The currently read row liveness infos (timestamp, ttl and localDeletionTime).
    private long rowTimestamp;
    private int rowTTL;
    private int rowLocalDeletionTime;

    private final ColumnFilter columnsToFetch;
    private ColumnFilter.Tester tester;

    public SerializationHelper(int version, Flag flag, ColumnFilter columnsToFetch)
    {
        this.flag = flag;
        this.version = version;
        this.columnsToFetch = columnsToFetch;
    }

    public SerializationHelper(int version, Flag flag)
    {
        this(version, flag, null);
    }

    public void writePartitionKeyLivenessInfo(Row.Writer writer, long timestamp, int ttl, int localDeletionTime)
    {
        livenessInfo.setTo(timestamp, ttl, localDeletionTime);
        writer.writePartitionKeyLivenessInfo(livenessInfo);

        rowTimestamp = timestamp;
        rowTTL = ttl;
        rowLocalDeletionTime = localDeletionTime;
    }

    public long getRowTimestamp()
    {
        return rowTimestamp;
    }

    public int getRowTTL()
    {
        return rowTTL;
    }

    public int getRowLocalDeletionTime()
    {
        return rowLocalDeletionTime;
    }

    public boolean includes(ColumnDefinition column)
    {
        return columnsToFetch == null || columnsToFetch.includes(column);
    }

    public boolean canSkipValue(ColumnDefinition column)
    {
        return columnsToFetch != null && columnsToFetch.canSkipValue(column);
    }

    public void startOfComplexColumn(ColumnDefinition column)
    {
        this.tester = columnsToFetch == null ? null : columnsToFetch.newTester(column);
    }

    public void endOfComplexColumn(ColumnDefinition column)
    {
        this.tester = null;
    }

    public void writeCell(Row.Writer writer,
                          ColumnDefinition column,
                          boolean isCounter,
                          ByteBuffer value,
                          long timestamp,
                          int localDelTime,
                          int ttl,
                          CellPath path)
    {
        livenessInfo.setTo(timestamp, ttl, localDelTime);

        if (isCounter && ((flag == Flag.FROM_REMOTE || (flag == Flag.LOCAL && CounterContext.instance().shouldClearLocal(value)))))
            value = CounterContext.instance().clearAllLocal(value);

        if (!column.isComplex() || tester == null || tester.includes(path))
        {
            if (tester != null && tester.canSkipValue(path))
                value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            writer.writeCell(column, isCounter, value, livenessInfo, path);
        }
    }
}
