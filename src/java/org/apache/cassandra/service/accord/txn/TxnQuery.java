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
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.api.Data.NOOP_DATA;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.CAS_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;

public abstract class TxnQuery implements Query
{

    /**
     * Used by transaction statements which will have Accord pass back to the C* coordinator code all the data that is
     * read even if it is not returned as part of the result to the client. TxnDataName.returning() will fetch the data
     * that is returned from TxnData.
     *
     * Also used by SERIAL key reads, and non-SERIAL key reads when they are executed on Accord.
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
                TxnRead txnKeyRead = (TxnRead)read;
                SinglePartitionReadCommand command = (SinglePartitionReadCommand) txnKeyRead.deserialize(0);
                // For CAS must return a non-empty result to indicate error even if there was no partition found
                return TxnData.of(txnDataName(CAS_READ), new TxnDataKeyValue(EmptyIterators.row(command.metadata(), command.partitionKey(), command.isReversed())));
            }
            else
                // If it failed to apply the partition contents are returned and it indicates failure
                return ((TxnData)data);
        }
    };

    /**
     * UNSAFE_EMPTY doesn't validate that the range is owned by Accord so you want to be careful and use NONE
     * if your transaction simply doesn't have results because that will validate that Accord owns the range
     * for things like blind writes. Empty is used by Accord for things like sync points which may need to execute
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

    public static final TxnQuery RANGE_QUERY = new TxnQuery()
    {
        @Override
        protected byte type()
        {
            return 5;
        }

        @Override
        public Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
        {
            return data != null ? concat((TxnData) data, read) : new TxnData();
        }

        private Result concat(TxnData data, Read read)
        {
            TxnRead txnRead = (TxnRead) read;
            PartitionRangeReadCommand command = (PartitionRangeReadCommand) txnRead.deserialize(0);
            TxnDataRangeValue value = (TxnDataRangeValue) data.get(txnDataName(TxnDataNameKind.USER));
            Supplier<PartitionIterator> source = value.toPartitionIterator(command.isReversed());
            // Because the query was split across multiple command stores the pushed down limit won't be sufficient
            // to return correct results and has to be applied again here
            Supplier<PartitionIterator> sourceWithLimits = () -> command.limits().filter(source.get(),
                                                                                         0,
                                                                                         command.selectsFullPartition(),
                                                                                         command.metadata().enforceStrictLiveness());
            return new TxnRangeReadResult(sourceWithLimits);
        }
    };

    private static final long SIZE = ObjectSizes.measure(ALL);

    private TxnQuery() {}

    abstract protected byte type();

    abstract protected Result doCompute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update);

    @Override
    public Result compute(TxnId txnId, Timestamp executeAt, Seekables<?, ?> keys, @Nullable Data data, @Nullable Read read, @Nullable Update update)
    {
        // TODO (required): This is not the cluster metadata of the current transaction
        ClusterMetadata clusterMetadata = ClusterMetadata.current();
        checkState(clusterMetadata.epoch.getEpoch() >= executeAt.epoch(), "TCM epoch %d is < executeAt epoch %d", clusterMetadata.epoch.getEpoch(), executeAt.epoch());
        boolean reads = read != null && !read.keys().isEmpty();
        if (transactionShouldBeBlocked(clusterMetadata, reads, keys, data, update))
        {
            if (txnId.isWrite())
                ClientRequestsMetricsHolder.accordWriteMetrics.accordMigrationRejects.mark();
            else
                ClientRequestsMetricsHolder.accordReadMetrics.accordMigrationRejects.mark();
            // Prevent writes from being applied
            if (update != null)
                ((TxnUpdate)update).failCondition();
            return RetryWithNewProtocolResult.instance;
        }
        return doCompute(txnId, executeAt, keys, data, read, update);
    }

    public long estimatedSizeOnHeap()
    {
        return SIZE;
    }

    public static final UnversionedSerializer<TxnQuery> serializer = new UnversionedSerializer<TxnQuery>()
    {
        @Override
        public void serialize(TxnQuery query, DataOutputPlus out) throws IOException
        {
            Preconditions.checkArgument(query == null | query == ALL | query == NONE | query == CONDITION | query == UNSAFE_EMPTY | query == RANGE_QUERY);
            out.writeByte(query == null ? 0 : query.type());
        }

        @Override
        public TxnQuery deserialize(DataInputPlus in) throws IOException
        {
            switch (in.readByte())
            {
                default: throw new AssertionError();
                case 0: return null;
                case 1: return ALL;
                case 2: return NONE;
                case 3: return CONDITION;
                case 4: return UNSAFE_EMPTY;
                case 5: return RANGE_QUERY;
            }
        }

        @Override
        public long serializedSize(TxnQuery query)
        {
            Preconditions.checkArgument(query == null | query == ALL | query == NONE | query == CONDITION | query == UNSAFE_EMPTY | query == RANGE_QUERY);
            return TypeSizes.sizeof((byte)2);
        }
    };

    private static boolean transactionShouldBeBlocked(ClusterMetadata clusterMetadata, boolean reads, Seekables<?, ?> keys, Data data, Update update)
    {
        if (data == NOOP_DATA && (update == null || update.keys().isEmpty()))
            return false;

        // TxnQuery needs to be smart enough to allow blind writes through for the non-transactional use cases during migration
        // This also allows blind write TransactionStatement to run before TransactionStatemetns with reads can run,
        // but this is harmless since we only promise that TransactionStatement works when migrated to Accord.
        // TODO (lowpri): This could look at read keys vs write keys to see if it can run in more cases
        if (reads)
            return !transactionIsSafeToReadAndWrite(clusterMetadata, keys);
        else
            return !transactionIsSafeToWrite(clusterMetadata, keys);
    }

    private static boolean transactionIsSafeToReadAndWrite(ClusterMetadata clusterMetadata, Seekables<?, ?> keys)
    {
        switch (keys.domain())
        {
            case Key:
                for (PartitionKey partitionKey : (Seekables<PartitionKey, ?>)keys)
                {
                    // TODO (required): This is looking at ClusterMetadata, but not the ClusterMetadata for the specified epoch, just that epoch or later. Need to store ConsensusMigrationState in the global Topologies Accord stores for itself.
                    if (!ConsensusRequestRouter.instance.isKeyManagedByAccordForReadAndWrite(clusterMetadata, partitionKey.table(), partitionKey.partitionKey()))
                        return false;
                }
                break;
            case Range:
                for (accord.primitives.Range range : (Ranges)keys)
                {
                    TokenRange tokenRange = (TokenRange)range;
                    // TODO (required): This is looking at ClusterMetadata, but not the ClusterMetadata for the specified epoch, just that epoch or later. Need to store ConsensusMigrationState in the global Topologies Accord stores for itself.
                    if (!ConsensusRequestRouter.instance.isRangeManagedByAccordForReadAndWrite(clusterMetadata, tokenRange.table(), tokenRange))
                        return false;
                }
                break;
            default:
                throw new IllegalStateException("Unsupported domain " + keys.domain());
        }

        return true;
    }

    private static boolean transactionIsSafeToWrite(ClusterMetadata clusterMetadata, Seekables<?, ?> keys)
    {
        checkState(keys.domain().isKey(), "Only key transactions are supported for writes");

        for (PartitionKey partitionKey : (Seekables<PartitionKey, ?>)keys)
        {
            // TODO (required): This is looking at ClusterMetadata, but not the ClusterMetadata for the specified epoch, just that epoch or later. Need to store ConsensusMigrationState in the global Topologies Accord stores for itself.
            if (!ConsensusRequestRouter.instance.isKeyManagedByAccordForWrite(clusterMetadata, partitionKey.table(), partitionKey.partitionKey()))
                return false;
        }
        return true;
    }
}
