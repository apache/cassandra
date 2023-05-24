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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.util.Counter;

public class VectorPostings<T>
{
    public static final long EMPTY_SIZE = ObjectSizes.measureDeep(new VectorPostings<Long>(0));
    public final int ordinal;
    public final List<T> postings;

    private final Counter bytesUsed = Counter.newCounter();

    // TODO refactor this so we can add the first posting at construction time instead of having
    // to append it separately (which will require a copy of the list)
    public VectorPostings(int ordinal)
    {
        this.ordinal = ordinal;
        // we expect that the overwhelmingly most common cardinality will be 1, so optimize for reads
        postings = new CopyOnWriteArrayList<>();
        bytesUsed.addAndGet(EMPTY_SIZE);
    }

    public void append(T key)
    {
        postings.add(key);
        // TODO how to get rid of measureDeep on the hot insert path?
        bytesUsed.addAndGet(ObjectSizes.measureDeep(key));
    }

    public long ramBytesUsed()
    {
        return bytesUsed.get();
    }
}
