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

package org.apache.cassandra.service.accord;

import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.local.MaxDecidedRX;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.index.accord.RouteIndexFormat;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.CloseableIterator;

public interface RangeSearcher
{
    Result search(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX);
    Result search(int commandStoreId, TokenKey key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX);

    static RangeSearcher extractRangeSearcher(Object o)
    {
        if (o instanceof RangeSearcher.Supplier)
            return ((RangeSearcher.Supplier) o).rangeSearcher();
        return NoopRangeSearcher.instance;
    }

    interface Supplier
    {
        RangeSearcher rangeSearcher();
    }

    interface Result
    {
        void consume(Consumer<TxnId> forEach);
        CloseableIterator<TxnId> results();
    }

    class DefaultResult implements Result
    {
        private final TxnId minTxnId;
        private final Timestamp maxTxnId;
        private final @Nullable MaxDecidedRX.DecidedRX decidedRX;
        private final CloseableIterator<TxnId> results;
        private boolean consumed = false;

        public DefaultResult(TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX, CloseableIterator<TxnId> results)
        {
            this.minTxnId = minTxnId;
            this.maxTxnId = maxTxnId;
            this.decidedRX = decidedRX;
            this.results = results;
        }

        @Override
        public CloseableIterator<TxnId> results()
        {
            consume();
            return results;
        }

        @Override
        public void consume(Consumer<TxnId> forEach)
        {
            consume();
            try (results)
            {
                while (results.hasNext())
                {
                    TxnId next = results.next();

                    if (next.compareTo(minTxnId) >= 0 && next.compareTo(maxTxnId) < 0 && RouteIndexFormat.includeByDecidedRX(decidedRX, next))
                        forEach.accept(next);
                }
            }
        }

        private void consume()
        {
            if (consumed)
                throw new IllegalStateException("Attempted to consume an already consumed result");
            consumed = true;
        }
    }

    enum NoopResult implements Result
    {
        instance;

        @Override
        public void consume(Consumer<TxnId> forEach)
        {

        }

        @Override
        public CloseableIterator<TxnId> results()
        {
            return CloseableIterator.empty();
        }
    }

    enum NoopRangeSearcher implements RangeSearcher
    {
        instance;

        @Override
        public Result search(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            return NoopResult.instance;
        }

        @Override
        public Result search(int commandStoreId, TokenKey key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
        {
            return NoopResult.instance;
        }
    }
}
