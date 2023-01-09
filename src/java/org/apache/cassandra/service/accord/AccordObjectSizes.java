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

import java.util.Map;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.Keys;
import accord.primitives.PartialKeyRoute;
import accord.primitives.PartialRangeRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.ObjectSizes;

public class AccordObjectSizes
{
    public static long key(Key key)
    {
        return ((PartitionKey) key).estimatedSizeOnHeap();
    }

    public static long key(RoutingKey key)
    {
        return ((AccordRoutingKey) key).estimatedSizeOnHeap();
    }

    private static final long EMPTY_KEY_RANGE_SIZE = ObjectSizes.measure(TokenRange.fullRange(""));
    public static long range(Range range)
    {
        return EMPTY_KEY_RANGE_SIZE + key(range.start()) + key(range.end());
    }

    private static final long EMPTY_KEY_RANGES_SIZE = ObjectSizes.measure(Ranges.of());
    public static long ranges(Ranges ranges)
    {
        long size = EMPTY_KEY_RANGES_SIZE;
        size += ObjectSizes.sizeOfReferenceArray(ranges.size());
        // TODO: many ranges are fixed size, can compute by multiplication
        for (int i = 0, mi = ranges.size() ; i < mi ; i++)
            size += range(ranges.get(i));
        return size;
    }

    private static final long EMPTY_KEYS_SIZE = ObjectSizes.measure(Keys.of());
    public static long keys(Keys keys)
    {
        long size = EMPTY_KEYS_SIZE;
        size += ObjectSizes.sizeOfReferenceArray(keys.size());
        for (int i=0, mi=keys.size(); i<mi; i++)
            size += key(keys.get(i));
        return size;
    }

    public static long seekables(Seekables<?, ?> seekables)
    {
        switch (seekables.kindOfContents())
        {
            default: throw new AssertionError();
            case Key: return keys((Keys) seekables);
            case Range: return ranges((Ranges) seekables);
        }
    }

    private static long routingKeysOnly(AbstractKeys<RoutingKey, ?> keys)
    {
        // TODO: many routing keys are fixed size, can compute by multiplication
        long size = ObjectSizes.sizeOfReferenceArray(keys.size());
        for (int i=0, mi=keys.size(); i<mi; i++)
            size += key(keys.get(i));
        return size;
    }

    private static final long EMPTY_ROUTING_KEYS_SIZE = ObjectSizes.measure(RoutingKeys.of());
    public static long routingKeys(RoutingKeys keys)
    {
        return EMPTY_ROUTING_KEYS_SIZE + routingKeysOnly(keys);
    }

    private static final long EMPTY_FULL_KEY_ROUTE_SIZE = ObjectSizes.measure(new FullKeyRoute(new TokenKey(null, null), new RoutingKey[0]));
    public static long fullKeyRoute(FullKeyRoute route)
    {
        return EMPTY_FULL_KEY_ROUTE_SIZE
               + routingKeysOnly(route)
               + key(route.homeKey()); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE = ObjectSizes.measure(new PartialKeyRoute(Ranges.EMPTY, new TokenKey(null, null), new RoutingKey[0]));
    public static long partialKeyRoute(PartialKeyRoute route)
    {
        return EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE
               + routingKeysOnly(route)
               + ranges(route.covering())
               + key(route.homeKey());
    }

    private static long rangesOnly(AbstractRanges<?> ranges)
    {
        long size = ObjectSizes.sizeOfReferenceArray(ranges.size());
        for (int i=0, mi=ranges.size(); i<mi; i++)
            size += range(ranges.get(i));
        return size;
    }

    private static final long EMPTY_FULL_RANGE_ROUTE_SIZE = ObjectSizes.measure(new FullRangeRoute(new TokenKey(null, null), new Range[0]));
    public static long fullRangeRoute(FullRangeRoute route)
    {
        return EMPTY_FULL_RANGE_ROUTE_SIZE
               + rangesOnly(route)
               + key(route.homeKey()); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE = ObjectSizes.measure(new PartialRangeRoute(Ranges.EMPTY, new TokenKey(null, null), new Range[0]));
    public static long partialRangeRoute(PartialRangeRoute route)
    {
        return EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE
               + rangesOnly(route)
               + ranges(route.covering())
               + key(route.homeKey());
    }

    public static long route(Unseekables<?, ?> unseekables)
    {
        switch (unseekables.kind())
        {
            default: throw new AssertionError();
            case RoutingKeys: return routingKeys((RoutingKeys) unseekables);
            case PartialKeyRoute: return partialKeyRoute((PartialKeyRoute) unseekables);
            case FullKeyRoute: return fullKeyRoute((FullKeyRoute) unseekables);
            case RoutingRanges: return ranges((Ranges) unseekables);
            case PartialRangeRoute: return partialRangeRoute((PartialRangeRoute) unseekables);
            case FullRangeRoute: return fullRangeRoute((FullRangeRoute) unseekables);
        }
    }

    private static final long EMPTY_TXN = ObjectSizes.measure(new PartialTxn.InMemory(null, null, null, null, null, null));
    public static long txn(PartialTxn txn)
    {
        long size = EMPTY_TXN;
        size += seekables(txn.keys());
        size += ((TxnRead) txn.read()).estimatedSizeOnHeap();
        if (txn.update() != null)
            size += ((TxnUpdate) txn.update()).estimatedSizeOnHeap();
        if (txn.query() != null)
            size += ((TxnQuery) txn.query()).estimatedSizeOnHeap();
        return size;
    }

    private static final long TIMESTAMP_SIZE = ObjectSizes.measureDeep(Timestamp.fromBits(0, 0, new Node.Id(0)));

    public static long timestamp()
    {
        return TIMESTAMP_SIZE;
    }
    public static long timestamp(Timestamp timestamp)
    {
        return TIMESTAMP_SIZE;
    }

    private static final long EMPTY_DEPS_SIZE = ObjectSizes.measureDeep(Deps.NONE);
    public static long dependencies(Deps dependencies)
    {
        long size = EMPTY_DEPS_SIZE;
        for (Map.Entry<Key, TxnId> entry : dependencies)
        {
            size += key(entry.getKey());
            size += timestamp(entry.getValue());
        }
        return size;
    }

    private static final long EMPTY_WRITES_SIZE = ObjectSizes.measure(new Writes(null, null, null));
    public static long writes(Writes writes)
    {
        long size = EMPTY_WRITES_SIZE;
        size += timestamp(writes.executeAt);
        size += keys(writes.keys);
        if (writes.write != null)
            size += ((TxnWrite) writes.write).estimatedSizeOnHeap();
        return size;
    }

}
