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

import java.util.Set;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import org.apache.lucene.util.Bits;

public class BitsUtil
{
    public static Bits bitsIgnoringDeleted(Bits toAccept, Set<Integer> deletedOrdinals)
    {
        return deletedOrdinals.isEmpty()
               ? toAccept
               : toAccept == null ? new NoDeletedBits(deletedOrdinals) : new NoDeletedIntersectingBits(toAccept, deletedOrdinals);
    }

    public static <T> Bits bitsIgnoringDeleted(Bits toAccept, NonBlockingHashMapLong<VectorPostings<T>> postings)
    {
        return toAccept == null ? new NoDeletedPostings(postings) : new NoDeletedIntersectingPostings(toAccept, postings);
    }

    private static class NoDeletedBits implements Bits
    {
        private final int length;
        private final Set<Integer> deletedOrdinals;

        private NoDeletedBits(Set<Integer> deletedOrdinals)
        {
            this.deletedOrdinals = deletedOrdinals;
            this.length = deletedOrdinals.stream().mapToInt(i -> i).max().orElse(0);
        }

        @Override
        public boolean get(int i)
        {
            return !deletedOrdinals.contains(i);
        }

        @Override
        public int length()
        {
            return length;
        }
    }

    private static class NoDeletedIntersectingBits implements Bits
    {
        private final Bits toAccept;
        private final Set<Integer> deletedOrdinals;
        private final int length;

        private NoDeletedIntersectingBits(Bits toAccept, Set<Integer> deletedOrdinals)
        {
            this.toAccept = toAccept;
            this.deletedOrdinals = deletedOrdinals;
            this.length = Math.max(toAccept.length(),
                                   deletedOrdinals.stream().mapToInt(i -> i).max().orElse(0));
        }

        @Override
        public boolean get(int i)
        {
            return !deletedOrdinals.contains(i) && toAccept.get(i);
        }

        @Override
        public int length()
        {
            return length;
        }
    }

    private static class NoDeletedPostings<T> implements Bits
    {
        private final NonBlockingHashMapLong<VectorPostings<T>> postings;

        public NoDeletedPostings(NonBlockingHashMapLong<VectorPostings<T>> postings)
        {
            this.postings = postings;
        }

        @Override
        public boolean get(int i)
        {
            var p = postings.get(i);
            assert p != null : "No postings for ordinal " + i;
            return !p.isEmpty();
        }

        @Override
        public int length()
        {
            return postings.size();
        }
    }

    private static class NoDeletedIntersectingPostings<T> implements Bits
    {
        private final Bits toAccept;
        private final NonBlockingHashMapLong<VectorPostings<T>> postings;

        public NoDeletedIntersectingPostings(Bits toAccept, NonBlockingHashMapLong<VectorPostings<T>> postings)
        {
            this.toAccept = toAccept;
            this.postings = postings;
        }

        @Override
        public boolean get(int i)
        {
            var p = postings.get(i);
            assert p != null : "No postings for ordinal " + i;
            return !p.isEmpty() && toAccept.get(i);
        }

        @Override
        public int length()
        {
            return postings.size();
        }
    }
}
