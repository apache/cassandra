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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.service.accord.txn.TxnRead.CAS_READ;

public abstract class TxnQuery implements Query
{
    /**
     * Used by transaction statements which will have Accord pass back to the C* coordinator code all the data that is
     * read even if it is not returned as part of the result to the client. TxnDataName.returning() will fetch the data
     * that is returned from TxnData.
     */
    public static final TxnQuery ALL = new TxnQuery()
    {
        @Override
        protected byte type()
        {
            return 1;
        }

        @Override
        public Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            return data != null ? (TxnData) data : new TxnData();
        }
    };

    /**
     * For transactions that return no results but do still care that they don't apply if the tokens/ranges
     * are not owned/managed by Accord
     */
    public static final TxnQuery NONE = new TxnQuery()
    {
        @Override
        protected byte type()
        {
            return 2;
        }

        @Override
        public Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            return new TxnData();
        }
    };

    /**
     * For supporting CQL CAS compatible transactions
     */
    public static final TxnQuery CONDITION = new TxnQuery()
    {
        @Override
        protected byte type()
        {
            return 3;
        }

        @Override
        public Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data,  @Nullable Read read, Update update)
        {
            checkNotNull(txnId, "txnId should not be null");
            checkNotNull(data, "data should not be null");
            checkNotNull(update, "update should not be null");

            AccordUpdate accordUpdate = (AccordUpdate)update;
            TxnData txnData = (TxnData)data;
            boolean conditionCheck = accordUpdate.checkCondition(data);
            // If the condition applied an empty result indicates success
            if (conditionCheck)
                return new TxnData();
            else if (txnData.isEmpty())
            {
                TxnRead txnRead = (TxnRead)read;
                SinglePartitionReadCommand command = (SinglePartitionReadCommand)txnRead.iterator().next().get();
                // For CAS must return a non-empty result to indicate error even if there was no partition found
                return new TxnData(ImmutableMap.of(CAS_READ, FilteredPartition.create(EmptyIterators.row(command.metadata(), command.partitionKey(), command.isReversed()))));
            }
            else
                // If it failed to apply the partition contents are returned and it indicates failure
                return ((TxnData)data);
        }
    };

    /**
     * UNSAFE_EMPTY doesn't validate that the range is owned by Accord so you want to be careful and use NONE
     * if your transaction simply doesn't have results because that will validate that Accord owns the range
     * for things like blind writes. Empty is used by Accord for things like sync points which may need to exeucte
     * for ranges Accord used to manage, but no longer does.
     */
    public static final TxnQuery UNSAFE_EMPTY = new TxnQuery()
    {

        @Override
        protected byte type()
        {
            return 4;
        }
        
        @Override
        public Result compute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            // Skip the migration checks in the base class for empty transactions, we don't
            // want/need the RetryWithNewProtocolResult
            return new TxnData();
        }

        @Override
        protected Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            throw new UnsupportedOperationException();
        }
    };

    private static final long SIZE = ObjectSizes.measure(ALL);

    private TxnQuery() {}

    abstract protected byte type();

    abstract protected Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update);

    @Override
    public Result compute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
    {
        Epoch epoch = Epoch.create(executeAt.epoch());
        if (transactionIsInMigratingOrMigratedRange(epoch, keys))
        {
            if (txnId.isWrite())
                ClientRequestsMetricsHolder.accordWriteMetrics.accordMigrationRejects.mark();
            else
                ClientRequestsMetricsHolder.accordReadMetrics.accordMigrationRejects.mark();
            return new RetryWithNewProtocolResult(epoch);
        }
        return doCompute(txnId, executeAt, keys, data, read, update);
    }

    public long estimatedSizeOnHeap()
    {
        return SIZE;
    }

    public static final IVersionedSerializer<TxnQuery> serializer = new IVersionedSerializer<TxnQuery>()
    {
        @Override
        public void serialize(TxnQuery query, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(query == null | query == ALL | query == NONE | query == CONDITION | query == UNSAFE_EMPTY);
            out.writeByte(query == null ? 0 : query.type());
        }

        @Override
        public TxnQuery deserialize(DataInputPlus in, int version) throws IOException
        {
            switch (in.readByte())
            {
                default: throw new AssertionError();
                case 0: return null;
                case 1: return ALL;
                case 2: return NONE;
                case 3: return CONDITION;
                case 4: return UNSAFE_EMPTY;
            }
        }

        @Override
        public long serializedSize(TxnQuery query, int version)
        {
            Preconditions.checkArgument(query == null | query == ALL | query == NONE | query == CONDITION | query == UNSAFE_EMPTY);
            return TypeSizes.sizeof((byte)2);
        }
    };

    private static boolean transactionIsInMigratingOrMigratedRange(Epoch epoch, Seekables<?, ?> keys)
    {
        // TODO (required): This is going to be problematic when we presumably support range reads and don't validate them
        // Whatever this transaction might be it isn't one supported for migration anyways
        if (!keys.domain().isKey())
            return false;

        for (PartitionKey partitionKey : (Seekables<PartitionKey, ?>)keys)
        {
            // TODO (required): This is looking at ClusterMetadata, but not the ClusterMetadata for the specified epoch, just that epoch or later. Need to store ConsensusMigrationState in the global Topologies Accord stores for itself.
            if (ConsensusRequestRouter.instance.isKeyInMigratingOrMigratedRangeFromAccord(epoch, partitionKey.table(), partitionKey.partitionKey()))
                return true;
        }
        return false;
    }
}
