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

import org.apache.lucene.util.RamUsageEstimator;

public class VectorPostings<T>
{
    private final List<T> postings;

    // TODO refactor this so we can add the first posting at construction time instead of having
    // to append it separately (which will require a copy of the list)
    public VectorPostings()
    {
        // we expect that the overwhelmingly most common cardinality will be 1, so optimize for reads
        postings = new CopyOnWriteArrayList<>();
    }

    public boolean add(T key)
    {
        for (T existing : postings)
            if (existing.equals(key))
                return false;
        postings.add(key);
        return true;
    }

    public long ramBytesUsed()
    {
        return emptyBytesUsed() + postings.size() * bytesPerPosting();
    }

    public static long emptyBytesUsed()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        return Integer.BYTES + REF_BYTES + AH_BYTES;
    }

    // we can't do this exactly without reflection, because keys could be Long or PrimaryKey.
    // PK is larger, so we'll take that and return an upper bound.
    // we already count the float[] vector in vectorValues, so leave it out here
    public static long bytesPerPosting()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return REF_BYTES
               + 2 * Long.BYTES // hashes in PreHashedDecoratedKey
               + REF_BYTES; // key ByteBuffer, this is used elsewhere so we don't take the deep size
    }

    public int size()
    {
        return postings.size();
    }

    public List<T> getPostings()
    {
        return postings;
    }
}
