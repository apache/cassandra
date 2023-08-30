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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.util.Set;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import io.github.jbellis.jvector.util.Bits;

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
        return toAccept == null ? new NoDeletedPostings<>(postings) : new NoDeletedIntersectingPostings<>(toAccept, postings);
    }

    private static abstract class BitsWithoutLength implements Bits, org.apache.lucene.util.Bits
    {
        @Override
        public int length()
        {
            // length() is not called on search path
            throw new UnsupportedOperationException();
        }
    }

    private static class NoDeletedBits extends BitsWithoutLength
    {
        private final Set<Integer> deletedOrdinals;

        private NoDeletedBits(Set<Integer> deletedOrdinals)
        {
            this.deletedOrdinals = deletedOrdinals;
        }

        @Override
        public boolean get(int i)
        {
            return !deletedOrdinals.contains(i);
        }
    }

    private static class NoDeletedIntersectingBits extends BitsWithoutLength
    {
        private final Bits toAccept;
        private final Set<Integer> deletedOrdinals;

        private NoDeletedIntersectingBits(Bits toAccept, Set<Integer> deletedOrdinals)
        {
            this.toAccept = toAccept;
            this.deletedOrdinals = deletedOrdinals;
        }

        @Override
        public boolean get(int i)
        {
            return !deletedOrdinals.contains(i) && toAccept.get(i);
        }
    }

    private static class NoDeletedPostings<T> extends BitsWithoutLength
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
    }

    private static class NoDeletedIntersectingPostings<T> extends BitsWithoutLength
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
    }
}
