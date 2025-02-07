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
package org.apache.cassandra.service.tracking;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

/**
 * As compactly as possible tracks mutation acks from replicas.
 */
public interface MutationAcks
{
    static MutationAcks create(Participants participants)
    {
        int size = participants.size();
        if (size <= 32)
            return new SmallAcks();
        else if (size <= 64)
            return new MediumAcks();
        else
            return new BigAcks(size);
    }

    void ack(int idx);

    boolean hasAcked(int idx);

    class SmallAcks extends AtomicInteger implements MutationAcks
    {
        @Override
        public void ack(int idx)
        {
            accumulateAndGet(1 << idx, (cur, mask) -> cur | mask);
        }

        @Override
        public boolean hasAcked(int idx)
        {
            int mask = 1 << idx;
            return mask == (get() & mask);
        }
    }

    class MediumAcks extends AtomicLong implements MutationAcks
    {
        @Override
        public void ack(int idx)
        {
            accumulateAndGet((long) 1 << idx, (cur, mask) -> cur | mask);
        }

        @Override
        public boolean hasAcked(int idx)
        {
            long mask = (long) 1 << idx;
            return mask == (get() & mask);
        }
    }

    class BigAcks extends AtomicIntegerArray implements MutationAcks
    {
        BigAcks(int size)
        {
            super(size / 32 + (size % 32 > 0 ? 1 : 0));
        }

        @Override
        public void ack(int idx)
        {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public boolean hasAcked(int idx)
        {
            throw new UnsupportedOperationException(); // TODO
        }
    }
}
