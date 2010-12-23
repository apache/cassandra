/**
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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A column that represents a partitioned counter.
 */
public class CounterColumn extends Column
{
    private static Logger logger = Logger.getLogger(CounterColumn.class);

    private static CounterContext contextManager = CounterContext.instance();

    protected ByteBuffer value;                 // NOT final: delta OR total of partitioned counter
    protected byte[] partitionedCounter;        // NOT final: only modify inline, carefully
    protected final long timestampOfLastDelete;

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
      this(name, value, timestamp, contextManager.create());
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp, byte[] partitionedCounter)
    {
      this(name, value, timestamp, partitionedCounter, Long.MIN_VALUE);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp, byte[] partitionedCounter, long timestampOfLastDelete)
    {
        super(name, FBUtilities.EMPTY_BYTE_BUFFER, timestamp);
        this.value = value;
        this.partitionedCounter = partitionedCounter;
        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    @Override
    public ByteBuffer value()
    {
        return value;
    }

    public byte[] partitionedCounter()
    {
        return partitionedCounter;
    }

    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    @Override
    public int size()
    {
        /*
         * An expired column adds to a Column : 
         *    4 bytes for length of partitionedCounter
         *  + length of partitionedCounter
         *  + 8 bytes for timestampOfLastDelete
         */
        return super.size() + DBConstants.intSize_ + partitionedCounter.length + DBConstants.tsSize_;
    }

    @Override
    public IColumn diff(IColumn column)
    {
        assert column instanceof CounterColumn : "Wrong class type.";

        if (timestamp() < column.timestamp())
            return column;
        if (timestampOfLastDelete() < ((CounterColumn)column).timestampOfLastDelete())
            return column;
        ContextRelationship rel = contextManager.diff(
            ((CounterColumn)column).partitionedCounter(),
            partitionedCounter());
        if (ContextRelationship.GREATER_THAN == rel || ContextRelationship.DISJOINT == rel)
            return column;
        return null;
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.array(),name.position()+name.arrayOffset(),name.remaining());
        digest.update(value.array(),value.position()+name.arrayOffset(),value.remaining());
        digest.update(FBUtilities.toByteArray(timestamp));
        digest.update(partitionedCounter);
        digest.update(FBUtilities.toByteArray(timestampOfLastDelete));
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        assert (column instanceof CounterColumn) || (column instanceof DeletedColumn) : "Wrong class type.";

        if (isMarkedForDelete())
        {
            if (column.isMarkedForDelete()) // tombstone + tombstone: keep later tombstone
            {
                return timestamp() > column.timestamp() ? this : column;
            }
            else // tombstone + live: track last tombstone
            {
                if (timestamp() > column.timestamp()) // tombstone > live
                {
                    return this;
                }
                // tombstone <= live last delete
                if (timestamp() <= ((CounterColumn)column).timestampOfLastDelete())
                {
                    return column;
                }
                // tombstone > live last delete
                return new CounterColumn(
                    column.name(),
                    column.value(),
                    column.timestamp(),
                    ((CounterColumn)column).partitionedCounter(),
                    timestamp());
            }
        }
        else if (column.isMarkedForDelete()) // live + tombstone: track last tombstone
        {
            if (timestamp() < column.timestamp()) // live < tombstone
            {
                return column;
            }
            // live last delete >= tombstone
            if (timestampOfLastDelete() >= column.timestamp())
            {
                return this;
            }
            // live last delete < tombstone
            return new CounterColumn(
                name(),
                value(),
                timestamp(),
                partitionedCounter(),
                column.timestamp());
        }
        // live + live: merge clocks; update value
        byte[] mergedPartitionedCounter = contextManager.merge(
            partitionedCounter(),
            ((CounterColumn)column).partitionedCounter());
		ByteBuffer byteBufferValue;
		if (0 == mergedPartitionedCounter.length)
		{
			long mergedValue = value().getLong(value().arrayOffset()) +
                               column.value().getLong(column.value().arrayOffset());
			byteBufferValue = FBUtilities.toByteBuffer(mergedValue);
		} else
			byteBufferValue = ByteBuffer.wrap(contextManager.total(mergedPartitionedCounter));
        return new CounterColumn(
            name(),
			byteBufferValue,
            Math.max(timestamp(), column.timestamp()),
            mergedPartitionedCounter,
            Math.max(timestampOfLastDelete(), ((CounterColumn)column).timestampOfLastDelete()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CounterColumn column = (CounterColumn)o;

        if (timestamp != column.timestamp)
            return false;

        if (timestampOfLastDelete != column.timestampOfLastDelete)
            return false;

        if (!Arrays.equals(partitionedCounter, column.partitionedCounter))
            return false;

        if (!name.equals(column.name))
            return false;

        return value.equals(column.value);
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (partitionedCounter != null ? Arrays.hashCode(partitionedCounter) : 0);
        result = 31 * result + (int)(timestampOfLastDelete ^ (timestampOfLastDelete >>> 32));
        return result;
    }

    @Override
    public IColumn deepCopy()
    {
        return new CounterColumn(
            ByteBufferUtil.clone(name),
            ByteBufferUtil.clone(value),
            timestamp,
            partitionedCounter,
            timestampOfLastDelete);
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(value.getLong(value.arrayOffset()));
        sb.append("@");
        sb.append(timestamp());
        sb.append("!");
        sb.append(timestampOfLastDelete);
        sb.append("@");
        sb.append(contextManager.toString(partitionedCounter));
        return sb.toString();
    }

    private void updateValue()
    {
        value = ByteBuffer.wrap(contextManager.total(partitionedCounter));
    }

    public void update(InetAddress node)
    {
        long delta = value.getLong(value.arrayOffset());
        partitionedCounter = contextManager.update(partitionedCounter, node, delta);
        updateValue();
    }

    public CounterColumn cleanNodeCounts(InetAddress node)
    {
        //XXX: inline modification non-destructive; cases:
        //     1) AES post-stream
        //     2) RRR, after CF.cloneMe()
        //     3) RRR, after CF.diff() which creates a new CF
        byte[] cleanPartitionedCounter  = contextManager.cleanNodeCounts(partitionedCounter, node);
        if (cleanPartitionedCounter == partitionedCounter)
            return this;
        if (0 == cleanPartitionedCounter.length)
            return null;
        return new CounterColumn(
            name,
            ByteBuffer.wrap(contextManager.total(cleanPartitionedCounter)),
            timestamp,
            cleanPartitionedCounter,
            timestampOfLastDelete
            );
    }
}

