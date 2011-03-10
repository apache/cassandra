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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NodeId;

/**
 * A column that represents a partitioned counter.
 */
public class CounterColumn extends Column
{
    private static final Logger logger = Logger.getLogger(CounterColumn.class);

    protected static final CounterContext contextManager = CounterContext.instance();

    private final long timestampOfLastDelete;

    public CounterColumn(ByteBuffer name, long value, long timestamp)
    {
        this(name, contextManager.create(value), timestamp);
    }

    public CounterColumn(ByteBuffer name, long value, long timestamp, long timestampOfLastDelete)
    {
        this(name, contextManager.create(value), timestamp, timestampOfLastDelete);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, Long.MIN_VALUE);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp, long timestampOfLastDelete)
    {
        super(name, value, timestamp);
        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    public long total()
    {
        return contextManager.total(value);
    }

    @Override
    public int size()
    {
        /*
         * A counter column adds to a Column :
         *  + 8 bytes for timestampOfLastDelete
         */
        return super.size() + DBConstants.tsSize_;
    }

    @Override
    public IColumn diff(IColumn column)
    {
        assert column instanceof CounterColumn : "Wrong class type.";

        if (timestamp() < column.timestamp())
            return column;
        if (timestampOfLastDelete() < ((CounterColumn)column).timestampOfLastDelete())
            return column;
        ContextRelationship rel = contextManager.diff(column.value(), value());
        if (ContextRelationship.GREATER_THAN == rel || ContextRelationship.DISJOINT == rel)
            return column;
        return null;
    }

    /*
     * We have to special case digest creation for counter column because
     * we don't want to include the information about which shard of the
     * context is a delta or not, since this information differs from node to
     * node.
     */
    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.duplicate());
        // We don't take the deltas into account in a digest
        contextManager.updateDigest(digest, value);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
            buffer.writeLong(timestampOfLastDelete);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public IColumn reconcile(IColumn column)
    {
        assert (column instanceof CounterColumn) || (column instanceof DeletedColumn) : "Wrong class type.";

        if (column.isMarkedForDelete()) // live + tombstone: track last tombstone
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
            return new CounterColumn(name(), value(), timestamp(), column.timestamp());
        }
        // live < live last delete
        if (timestamp() < ((CounterColumn)column).timestampOfLastDelete())
            return column;
        // live last delete > live
        if (timestampOfLastDelete() > column.timestamp())
            return this;
        // live + live: merge clocks; update value
        return new CounterColumn(
            name(),
            contextManager.merge(value(), column.value()),
            Math.max(timestamp(), column.timestamp()),
            Math.max(timestampOfLastDelete(), ((CounterColumn)column).timestampOfLastDelete()));
    }

    @Override
    public boolean equals(Object o)
    {
        // super.equals() returns false if o is not a CounterColumn
        return super.equals(o) && timestampOfLastDelete == ((CounterColumn)o).timestampOfLastDelete;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int)(timestampOfLastDelete ^ (timestampOfLastDelete >>> 32));
        return result;
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs)
    {
        return new CounterColumn(cfs.internOrCopy(name), ByteBufferUtil.clone(value), timestamp, timestampOfLastDelete);
    }

    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(contextManager.toString(value));
        sb.append("@");
        sb.append(timestamp());
        sb.append("!");
        sb.append(timestampOfLastDelete);
        return sb.toString();
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_MASK;
    }

    /**
     * Check if a given nodeId is found in this CounterColumn context.
     */
    public boolean hasNodeId(NodeId id)
    {
        return contextManager.hasNodeId(value(), id);
    }

    public CounterColumn computeOldShardMerger()
    {
        ByteBuffer bb = contextManager.computeOldShardMerger(value(), NodeId.getOldLocalNodeIds());
        if (bb == null)
            return null;
        else
            return new CounterColumn(name(), bb, timestamp(), timestampOfLastDelete);
    }

    private CounterColumn removeOldShards(int gcBefore)
    {
        ByteBuffer bb = contextManager.removeOldShards(value(), gcBefore);
        if (bb == value())
            return this;
        else
        {
            return new CounterColumn(name(), bb, timestamp(), timestampOfLastDelete);
        }
    }

    public static void removeOldShards(ColumnFamily cf, int gcBefore)
    {
        if (!cf.isSuper())
        {
            for (Map.Entry<ByteBuffer, IColumn> entry : cf.getColumnsMap().entrySet())
            {
                ByteBuffer cname = entry.getKey();
                IColumn c = entry.getValue();
                if (!(c instanceof CounterColumn))
                    continue;
                CounterColumn cleaned = ((CounterColumn) c).removeOldShards(gcBefore);
                if (cleaned != c)
                {
                    cf.remove(cname);
                    cf.addColumn(cleaned);
                }
            }
        }
        else
        {
            for (Map.Entry<ByteBuffer, IColumn> entry : cf.getColumnsMap().entrySet())
            {
                SuperColumn c = (SuperColumn) entry.getValue();
                for (IColumn subColumn : c.getSubColumns())
                {
                    if (!(subColumn instanceof CounterColumn))
                        continue;
                    CounterColumn cleaned = ((CounterColumn) subColumn).removeOldShards(gcBefore);
                    if (cleaned != subColumn)
                    {
                        c.remove(subColumn.name());
                        c.addColumn(cleaned);
                    }
                }
            }
        }
    }
}
