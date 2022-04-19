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
import accord.local.Node;
import accord.txn.Dependencies;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.db.AccordQuery;
import org.apache.cassandra.service.accord.db.AccordRead;
import org.apache.cassandra.service.accord.db.AccordUpdate;
import org.apache.cassandra.service.accord.db.AccordWrite;
import org.apache.cassandra.utils.ObjectSizes;

public class AccordObjectSizes
{
    private static final long EMPTY_KEYS_SIZE = ObjectSizes.measure(new Keys(new Key[0]));
    public static long keys(Keys keys)
    {
        long size = EMPTY_KEYS_SIZE;
        size += ObjectSizes.sizeOfReferenceArray(keys.size());
        for (int i=0, mi=keys.size(); i<mi; i++)
            size += ((AccordKey.PartitionKey) keys.get(i)).estimatedSizeOnHeap();
        return size;
    }

    private static final long EMPTY_TXN = ObjectSizes.measure(new Txn(null, null, null));
    public static long txn(Txn txn)
    {
        long size = EMPTY_TXN;
        size += keys(txn.keys());
        size += ((AccordRead) txn.read).estimatedSizeOnHeap();
        if (txn.update != null)
            size += ((AccordUpdate) txn.update).estimatedSizeOnHeap();
        size += ((AccordQuery) txn.query).estimatedSizeOnHeap();
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

    private static final long EMPTY_DEPS_SIZE = ObjectSizes.measureDeep(new Dependencies());
    public static long dependencies(Dependencies dependencies)
    {
        long size = EMPTY_DEPS_SIZE;
        for (Map.Entry<TxnId, Txn> entry : dependencies)
        {
            size += timestamp(entry.getKey());
            size += txn(entry.getValue());
        }
        return size;
    }

    private static final long EMPTY_WRITES_SIZE = ObjectSizes.measure(new Writes(null, null, null));
    public static long writes(Writes writes)
    {
        long size = EMPTY_WRITES_SIZE;
        size += timestamp(writes.executeAt);
        size += keys(writes.keys);
        size += ((AccordWrite) writes.write).estimatedSizeOnHeap();
        return size;
    }

}
