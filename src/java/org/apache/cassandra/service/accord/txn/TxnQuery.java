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

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.config.Config.LegacyPaxosStrategy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.service.ConsensusRequestRouter;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class TxnQuery implements Query
{
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

            TxnUpdate txnUpdate = (TxnUpdate)update;
            boolean conditionCheck = txnUpdate.checkCondition(data);
            // If the condition applied an empty result indicates success
            if (conditionCheck)
                return new TxnData();
            else
                // If it failed to apply the partition contents (if present) are returned and it indicates failure
                return ((TxnData)data);
        }
    };

    public static final TxnQuery EMPTY = new TxnQuery()
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
        Epoch epoch = Epoch.create(0, executeAt.epoch());
        // TODO This isn't attempting to block transaction statements although it will catch the single key ones
        // That will need to be tackled as part of interoperability
        if (transactionIsInMigratingOrMigratedRange(epoch, keys))
        {
            // Fail fast because we can't be sure where this request should really run or what was intended
            if (DatabaseDescriptor.getLegacyPaxosStrategy() == LegacyPaxosStrategy.accord)
                throw new IllegalStateException("Mixing a hard coded strategy with migration is unsupported");

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
            Preconditions.checkArgument(query == null | query == ALL | query == NONE | query == CONDITION | query == EMPTY);
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
                case 4: return EMPTY;
            }
        }

        @Override
        public long serializedSize(TxnQuery query, int version)
        {
            Preconditions.checkArgument(query == null | query == ALL | query == NONE | query == CONDITION | query == EMPTY);
            return TypeSizes.sizeof((byte)2);
        }
    };

    private static boolean transactionIsInMigratingOrMigratedRange(Epoch epoch, Seekables<?, ?> keys)
    {
        // Whatever this transaction might be it isn't one supported for migration anyways
        if (!keys.domain().isKey())
            return false;

        if (keys.size() > 1)
            // It has to be a transaction statement and we don't support migration with those
            return false;
        // Could be a transaction statement, but this check does no additional harm
        // and transaction statement will generate an error when it sees
        // the RetryOnNewProtocolResult
        PartitionKey partitionKey = (PartitionKey)keys.get(0);
        return ConsensusRequestRouter.instance.isKeyInMigratingOrMigratedRangeFromAccord(epoch, partitionKey.tableId(), partitionKey.partitionKey());
    }
}
