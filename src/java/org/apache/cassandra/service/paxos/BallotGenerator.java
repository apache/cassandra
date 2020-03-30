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

package org.apache.cassandra.service.paxos;

import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.paxos.Ballot.Flag;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.service.paxos.Ballot.atUnixMicrosWithLsb;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface BallotGenerator
{
    static class Default implements BallotGenerator
    {
        private static final SecureRandom secureRandom = new SecureRandom();

        public Ballot atUnixMicros(long unixMicros, Flag flag)
        {
            return atUnixMicrosWithLsb(unixMicros, secureRandom.nextLong(), flag);
        }

        public Ballot next(long minUnixMicros, Flag flag)
        {
            long unixMicros = ClientState.getTimestampForPaxos(minUnixMicros);
            return atUnixMicros(unixMicros, flag);
        }

        public Ballot stale(long fromInMicros, long toInMicros, Flag flag)
        {
            long unixMicros = ThreadLocalRandom.current().nextLong(fromInMicros, toInMicros);
            return atUnixMicros(unixMicros, flag);
        }

        public long next(long minTimestamp)
        {
            return ClientState.getTimestampForPaxos(minTimestamp);
        }

        public long prevUnixMicros()
        {
            return ClientState.getLastTimestampMicros();
        }
    }

    static class Global
    {
        private static BallotGenerator instance = new Default();
        public static Ballot atUnixMicros(long unixMicros, Flag flag) { return instance.atUnixMicros(unixMicros, flag); }
        public static Ballot nextBallot(Flag flag) { return instance.next(Long.MIN_VALUE, flag); }
        public static Ballot nextBallot(long minUnixMicros, Flag flag) { return instance.next(minUnixMicros, flag); }
        public static Ballot staleBallot(long fromUnixMicros, long toUnixMicros, Flag flag) { return instance.stale(fromUnixMicros, toUnixMicros, flag); }
        public static long prevUnixMicros() { return instance.prevUnixMicros(); }

        public static void unsafeSet(BallotGenerator newInstance)
        {
            instance = newInstance;
        }
    }

    Ballot atUnixMicros(long unixMicros, Flag flag);
    Ballot next(long minUnixMicros, Flag flag);
    Ballot stale(long fromUnixMicros, long toUnixMicros, Flag flag);
    long prevUnixMicros();
}