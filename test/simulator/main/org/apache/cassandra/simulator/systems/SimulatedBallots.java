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

package org.apache.cassandra.simulator.systems;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.RandomSource.Choices;

import static org.apache.cassandra.service.paxos.Ballot.atUnixMicrosWithLsb;

// TODO (feature): link with SimulateTime, and otherwise improve
public class SimulatedBallots
{
    enum Next { ONE, JUMP, TO_LATEST }

    final LongSupplier uniqueSupplier; // must be unique for all ballots
    final RandomSource random;
    final Choices<Next> nextChoice;
    final LongSupplier nextJump;
    final AtomicLong latest = new AtomicLong(1L);

    public SimulatedBallots(RandomSource random, Supplier<LongSupplier> jumpsSupplier)
    {
        this.uniqueSupplier = random.uniqueUniformSupplier(Long.MIN_VALUE, Long.MAX_VALUE);
        this.random = random;
        this.nextChoice = Choices.random(random, Next.values());
        this.nextJump = jumpsSupplier.get();
    }

    class Generator extends AtomicLong implements BallotGenerator
    {
        public Generator()
        {
            super(1L);
        }

        public Ballot atUnixMicros(long unixMicros, Ballot.Flag flag)
        {
            return atUnixMicrosWithLsb(unixMicros, uniqueSupplier.getAsLong(), flag);
        }

        public Ballot next(long minUnixMicros, Ballot.Flag flag)
        {
            return Ballot.atUnixMicrosWithLsb(nextBallotTimestampMicros(minUnixMicros), uniqueSupplier.getAsLong(), flag);
        }

        public Ballot stale(long from, long to, Ballot.Flag flag)
        {
            return Ballot.atUnixMicrosWithLsb(random.uniform(from, to), uniqueSupplier.getAsLong(), flag);
        }

        private long nextBallotTimestampMicros(long minUnixMicros)
        {
            long next;
            switch (nextChoice.choose(random))
            {
                default: throw new IllegalStateException();
                case TO_LATEST:
                    minUnixMicros = Math.max(latest.get(), minUnixMicros);
                case ONE:
                    next = accumulateAndGet(minUnixMicros, (a, b) -> Math.max(a, b) + 1);
                    break;
                case JUMP:
                    long jump = Math.max(1, nextJump.getAsLong());
                    next = addAndGet(jump);
                    if (next < minUnixMicros)
                        next = accumulateAndGet(minUnixMicros, (a, b) -> Math.max(a, b) + 1);
            }
            latest.accumulateAndGet(next, Math::max);
            return next;
        }

        public long prevUnixMicros()
        {
            return get();
        }
    }

    public BallotGenerator get()
    {
        return new Generator();
    }
}
