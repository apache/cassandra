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

package org.apache.cassandra.index.accord;

import java.util.function.LongFunction;

import accord.primitives.TxnId;
import accord.utils.RandomSource;

import static org.apache.cassandra.index.accord.AccordIndexUtil.normalize;

public class TxnRange
{
    public static final TxnRange FULL = new TxnRange(TxnId.NONE, TxnId.MAX);

    public final TxnId minTxnId;
    public final TxnId maxTxnId;

    public TxnRange(TxnId minTxnId, TxnId maxTxnId)
    {
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
    }

    public boolean includes(TxnId other)
    {
        return minTxnId.compareTo(other) <= 0 && other.compareTo(maxTxnId) <= 0;
    }

    public static TxnRange next(RandomSource rs, long minKnown, long maxKnown, LongFunction<TxnId> idFor)
    {
        TxnId minTxnId;
        TxnId maxTxnId;
        if (minKnown == maxKnown)
        {
            // just do random
            minTxnId = idFor.apply(rs.nextLong(1, 1 << 16));
            maxTxnId = idFor.apply(rs.nextLong(1, 1 << 16));
        }
        else
        {
            switch (rs.nextInt(0, 3))
            {
                case 0: // future
                {
                    minTxnId = idFor.apply(maxKnown + 10);
                    maxTxnId = idFor.apply(maxKnown + 100);
                }
                break;
                case 1: // past
                {
                    minTxnId = idFor.apply(Math.max(1, minKnown - 100));
                    maxTxnId = idFor.apply(Math.max(1, minKnown - 10));
                }
                break;
                case 2: // present-ish
                {
                    // this can cause min/max to be reversed!
                    minTxnId = idFor.apply(Math.max(1, minKnown + 10));
                    maxTxnId = idFor.apply(Math.max(1, maxKnown - 10));
                }
                break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return new TxnRange(minTxnId, maxTxnId);
    }

    @Override
    public String toString()
    {
        return normalize(minTxnId) + ',' + normalize(maxTxnId);
    }
}
