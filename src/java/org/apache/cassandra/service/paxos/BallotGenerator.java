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

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface BallotGenerator
{
    static class Default implements BallotGenerator
    {
        public UUID randomBallot(long whenInMicros, boolean isSerial)
        {
            return UUIDGen.getRandomTimeUUIDFromMicros(whenInMicros, isSerial ? 2 : 1);
        }

        public UUID randomBallot(long fromInMicros, long toInMicros, boolean isSerial)
        {
            long timestampMicros = ThreadLocalRandom.current().nextLong(fromInMicros, toInMicros);
            return randomBallot(timestampMicros, isSerial);
        }

        public long nextBallotTimestampMicros(long minTimestamp)
        {
            return ClientState.getTimestampForPaxos(minTimestamp);
        }

        public long prevBallotTimestampMicros()
        {
            return ClientState.getLastTimestampMicros();
        }
    }

    static class Global
    {
        private static BallotGenerator instance = new Default();
        public static UUID randomBallot(long whenInMicros, boolean isSerial) { return instance.randomBallot(whenInMicros, isSerial); }
        public static UUID randomBallot(long fromInMicros, long toInMicros, boolean isSerial) { return instance.randomBallot(fromInMicros, toInMicros, isSerial); }
        public static long nextBallotTimestampMicros(long minWhenInMicros) { return instance.nextBallotTimestampMicros(minWhenInMicros); }
        public static long prevBallotTimestampMicros() { return instance.prevBallotTimestampMicros(); }

        public static void unsafeSet(BallotGenerator newInstance)
        {
            instance = newInstance;
        }
    }

    UUID randomBallot(long whenInMicros, boolean isSerial);
    UUID randomBallot(long fromInMicros, long toInMicros, boolean isSerial);
    long nextBallotTimestampMicros(long minWhenInMicros);
    long prevBallotTimestampMicros();
}