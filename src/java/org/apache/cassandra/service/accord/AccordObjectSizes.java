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
import accord.primitives.AbstractRoute;
import accord.primitives.Deps;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.db.AccordQuery;
import org.apache.cassandra.service.accord.db.AccordRead;
import org.apache.cassandra.service.accord.db.AccordUpdate;
import org.apache.cassandra.service.accord.db.AccordWrite;
import org.apache.cassandra.utils.ObjectSizes;

public class AccordObjectSizes
{
    public static long key(Key key)
    {
        return ((AccordKey.PartitionKey) key).estimatedSizeOnHeap();
    }

    public static long key(RoutingKey key)
    {
        return ((AccordRoutingKey) key).estimatedSizeOnHeap();
    }

    private static final long EMPTY_KEY_RANGE_SIZE = ObjectSizes.measure(TokenRange.fullRange(TableId.generate()));
    public static long range(KeyRange range)
    {
        return EMPTY_KEY_RANGE_SIZE + key(range.start()) + key(range.end());
    }

    private static final long EMPTY_KEY_RANGES_SIZE = ObjectSizes.measure(KeyRanges.of());
    public static long ranges(KeyRanges ranges)
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

    private static long routingKeysOnly(RoutingKeys keys)
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
        return routingKeysOnly(keys) + EMPTY_ROUTING_KEYS_SIZE;
    }

    private static final long EMPTY_ROUTE_SIZE = ObjectSizes.measure(new Route(new TokenKey(null, null), new RoutingKey[0]));
    public static long route(Route route)
    {
        return EMPTY_ROUTE_SIZE
               + routingKeysOnly(route)
               + key(route.homeKey); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_ROUTE_KEYS_SIZE = ObjectSizes.measure(new PartialRoute(KeyRanges.EMPTY, new TokenKey(null, null), new RoutingKey[0]));
    public static long route(PartialRoute route)
    {
        return EMPTY_PARTIAL_ROUTE_KEYS_SIZE
               + routingKeysOnly(route)
               + ranges(route.covering)
               + key(route.homeKey);
    }

    public static long route(AbstractRoute route)
    {
        if (route instanceof Route) return route((Route) route);
        else return route((PartialRoute) route);
    }

    private static final long EMPTY_TXN = ObjectSizes.measure(new PartialTxn.InMemory(null, null, null, null, null, null));
    public static long txn(PartialTxn txn)
    {
        long size = EMPTY_TXN;
        size += keys(txn.keys());
        size += ((AccordRead) txn.read()).estimatedSizeOnHeap();
        if (txn.update() != null)
            size += ((AccordUpdate) txn.update()).estimatedSizeOnHeap();
        if (txn.query() != null)
            size += ((AccordQuery) txn.query()).estimatedSizeOnHeap();
        return size;
    }

    private static final long TIMESTAMP_SIZE = ObjectSizes.measureDeep(new Timestamp(0, 0, 0, new Node.Id(0)));

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
            size += ((AccordWrite) writes.write).estimatedSizeOnHeap();
        return size;
    }

}
