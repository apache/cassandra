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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.serializers.MarshalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.*;

/**
 * A column that represents a partitioned counter.
 */
public class CounterCell extends Cell
{
    private static final Logger logger = LoggerFactory.getLogger(CounterCell.class);

    protected static final CounterContext contextManager = CounterContext.instance();

    private final long timestampOfLastDelete;

    public CounterCell(CellName name, long value, long timestamp)
    {
        this(name, contextManager.create(value, HeapAllocator.instance), timestamp);
    }

    public CounterCell(CellName name, long value, long timestamp, long timestampOfLastDelete)
    {
        this(name, contextManager.create(value, HeapAllocator.instance), timestamp, timestampOfLastDelete);
    }

    public CounterCell(CellName name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, Long.MIN_VALUE);
    }

    public CounterCell(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete)
    {
        super(name, value, timestamp);
        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    public static CounterCell create(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete, ColumnSerializer.Flag flag)
    {
        // #elt being negative means we have to clean delta
        short count = value.getShort(value.position());
        if (flag == ColumnSerializer.Flag.FROM_REMOTE || (flag == ColumnSerializer.Flag.LOCAL && count < 0))
            value = CounterContext.instance().clearAllDelta(value);
        return new CounterCell(name, value, timestamp, timestampOfLastDelete);
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new CounterCell(newName, value, timestamp, timestampOfLastDelete);
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
    public int dataSize()
    {
        /*
         * A counter column adds to a Cell :
         *  + 8 bytes for timestampOfLastDelete
         */
        return super.dataSize() + TypeSizes.NATIVE.sizeof(timestampOfLastDelete);
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return super.serializedSize(type, typeSizes) + typeSizes.sizeof(timestampOfLastDelete);
    }

    @Override
    public Cell diff(Cell cell)
    {
        assert (cell instanceof CounterCell) || (cell instanceof DeletedCell) : "Wrong class type: " + cell.getClass();

        if (timestamp() < cell.timestamp())
            return cell;

        // Note that if at that point, cell can't be a tombstone. Indeed,
        // cell is the result of merging us with other nodes results, and
        // merging a CounterCell with a tombstone never return a tombstone
        // unless that tombstone timestamp is greater that the CounterCell
        // one.
        assert !(cell instanceof DeletedCell) : "Wrong class type: " + cell.getClass();

        if (timestampOfLastDelete() < ((CounterCell) cell).timestampOfLastDelete())
            return cell;
        ContextRelationship rel = contextManager.diff(cell.value(), value());
        if (ContextRelationship.GREATER_THAN == rel || ContextRelationship.DISJOINT == rel)
            return cell;
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
        digest.update(name.toByteBuffer().duplicate());
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
    public Cell reconcile(Cell cell, Allocator allocator)
    {
        assert (cell instanceof CounterCell) || (cell instanceof DeletedCell) : "Wrong class type: " + cell.getClass();

        // live + tombstone: track last tombstone
        if (cell.isMarkedForDelete(Long.MIN_VALUE)) // cannot be an expired cell, so the current time is irrelevant
        {
            // live < tombstone
            if (timestamp() < cell.timestamp())
            {
                return cell;
            }
            // live last delete >= tombstone
            if (timestampOfLastDelete() >= cell.timestamp())
            {
                return this;
            }
            // live last delete < tombstone
            return new CounterCell(name(), value(), timestamp(), cell.timestamp());
        }
        // live < live last delete
        if (timestamp() < ((CounterCell) cell).timestampOfLastDelete())
            return cell;
        // live last delete > live
        if (timestampOfLastDelete() > cell.timestamp())
            return this;
        // live + live: merge clocks; update value
        return new CounterCell(
            name(),
            contextManager.merge(value(), cell.value(), allocator),
            Math.max(timestamp(), cell.timestamp()),
            Math.max(timestampOfLastDelete(), ((CounterCell) cell).timestampOfLastDelete()));
    }

    @Override
    public boolean equals(Object o)
    {
        // super.equals() returns false if o is not a CounterCell
        return super.equals(o) && timestampOfLastDelete == ((CounterCell)o).timestampOfLastDelete;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int)(timestampOfLastDelete ^ (timestampOfLastDelete >>> 32));
        return result;
    }

    @Override
    public Cell localCopy(ColumnFamilyStore cfs)
    {
        return localCopy(cfs, HeapAllocator.instance);
    }

    @Override
    public Cell localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new CounterCell(name.copy(allocator), allocator.clone(value), timestamp, timestampOfLastDelete);
    }

    @Override
    public String getString(CellNameType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(false);
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

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
        // which is not the internal representation of counters
        contextManager.validateContext(value());
    }

    /**
     * Check if a given counterId is found in this CounterCell context.
     */
    public boolean hasCounterId(CounterId id)
    {
        return contextManager.hasCounterId(value(), id);
    }

    private CounterCell computeOldShardMerger(int mergeBefore)
    {
        ByteBuffer bb = contextManager.computeOldShardMerger(value(), CounterId.getOldLocalCounterIds(), mergeBefore);
        if (bb == null)
            return null;
        else
            return new CounterCell(name(), bb, timestamp(), timestampOfLastDelete);
    }

    private CounterCell removeOldShards(int gcBefore)
    {
        ByteBuffer bb = contextManager.removeOldShards(value(), gcBefore);
        if (bb == value())
            return this;
        else
        {
            return new CounterCell(name(), bb, timestamp(), timestampOfLastDelete);
        }
    }

    public static void mergeAndRemoveOldShards(DecoratedKey key, ColumnFamily cf, int gcBefore, int mergeBefore)
    {
        mergeAndRemoveOldShards(key, cf, gcBefore, mergeBefore, true);
    }

    /**
     * There is two phase to the removal of old shards.
     * First phase: we merge the old shard value to the current shard and
     * 'nulify' the old one. We then send the counter context with the old
     * shard nulified to all other replica.
     * Second phase: once an old shard has been nulified for longer than
     * gc_grace (to be sure all other replica had been aware of the merge), we
     * simply remove that old shard from the context (it's value is 0).
     * This method does both phases.
     * (Note that the sendToOtherReplica flag is here only to facilitate
     * testing. It should be true in real code so use the method above
     * preferably)
     */
    public static void mergeAndRemoveOldShards(DecoratedKey key, ColumnFamily cf, int gcBefore, int mergeBefore, boolean sendToOtherReplica)
    {
        ColumnFamily remoteMerger = null;

        for (Cell c : cf)
        {
            if (!(c instanceof CounterCell))
                continue;
            CounterCell cc = (CounterCell) c;
            CounterCell shardMerger = cc.computeOldShardMerger(mergeBefore);
            CounterCell merged = cc;
            if (shardMerger != null)
            {
                merged = (CounterCell) cc.reconcile(shardMerger);
                if (remoteMerger == null)
                    remoteMerger = cf.cloneMeShallow();
                remoteMerger.addColumn(merged);
            }
            CounterCell cleaned = merged.removeOldShards(gcBefore);
            if (cleaned != cc)
            {
                cf.replace(cc, cleaned);
            }
        }

        if (remoteMerger != null && sendToOtherReplica)
        {
            try
            {
                sendToOtherReplica(key, remoteMerger);
            }
            catch (Exception e)
            {
                logger.error("Error while sending shard merger mutation to remote endpoints", e);
            }
        }
    }

    public Cell markDeltaToBeCleared()
    {
        return new CounterCell(name, contextManager.markDeltaToBeCleared(value), timestamp, timestampOfLastDelete);
    }

    private static void sendToOtherReplica(DecoratedKey key, ColumnFamily cf) throws RequestExecutionException
    {
        Mutation mutation = new Mutation(cf.metadata().ksName, key.key, cf);

        final InetAddress local = FBUtilities.getBroadcastAddress();
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(local);

        StorageProxy.performWrite(mutation, ConsistencyLevel.ANY, localDataCenter, new StorageProxy.WritePerformer()
        {
            public void apply(IMutation mutation, Iterable<InetAddress> targets, AbstractWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level)
            throws OverloadedException
            {
                // We should only send to the remote replica, not the local one
                Set<InetAddress> remotes = Sets.difference(ImmutableSet.copyOf(targets), ImmutableSet.of(local));
                // Fake local response to be a good lad but we won't wait on the responseHandler
                responseHandler.response(null);
                StorageProxy.sendToHintedEndpoints((Mutation) mutation, remotes, responseHandler, localDataCenter);
            }
        }, null, WriteType.SIMPLE);

        // we don't wait for answers
    }
}
