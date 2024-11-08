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

package org.apache.cassandra.harry.gen.rng;

public interface PureRng
{
    long randomNumber(long i, long stream);

    long sequenceNumber(long r, long stream);

    default long next(long r)
    {
        return next(r, 0);
    }

    long next(long r, long stream);

    long prev(long r, long stream);

    default long prev(long r)
    {
        return next(r, 0);
    }

    class PCGFast implements PureRng
    {
        private final long seed;

        public PCGFast(long seed)
        {
            this.seed = seed;
        }

        public long randomNumber(long i, long stream)
        {
            return PCGFastPure.shuffle(PCGFastPure.advanceState(seed, i, stream));
        }

        public long sequenceNumber(long r, long stream)
        {
            return PCGFastPure.distance(seed, PCGFastPure.unshuffle(r), stream);
        }

        public long next(long r, long stream)
        {
            return PCGFastPure.next(r, stream);
        }

        public long prev(long r, long stream)
        {
            return PCGFastPure.previous(r, stream);
        }
    }
}