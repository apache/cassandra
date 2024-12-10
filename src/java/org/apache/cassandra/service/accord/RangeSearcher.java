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

import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

public interface RangeSearcher
{
    void intersects(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach);
    void intersects(int commandStoreId, AccordRoutingKey key, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach);

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

    enum NoopRangeSearcher implements RangeSearcher
    {
        instance;

        @Override
        public void intersects(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
        {

        }

        @Override
        public void intersects(int commandStoreId, AccordRoutingKey key, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
        {

        }
    }
}
