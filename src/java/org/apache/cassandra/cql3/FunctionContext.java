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

package org.apache.cassandra.cql3;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionArguments;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.TimeUUID;

public interface FunctionContext
{
    NoTimeFunctionContext NONE = () -> { throw new UnsupportedOperationException(); };

    interface RealTimeFunctionContext extends FunctionContext
    {
        @Override default byte[] nextTimeUUIDAsBytes() { return TimeUUID.Generator.nextTimeUUIDAsBytes(); }
        @Override default Instant now() { return Instant.now(Clock.system(ZoneOffset.UTC)); }
        @Override default long nowMicros() { return TimeUUID.Generator.nextUnixMicros(); }
        @Override default long nowMillis() { return System.currentTimeMillis(); }
    }

    interface NoTimeFunctionContext extends FunctionContext
    {
        @Override default byte[] nextTimeUUIDAsBytes() { throw new UnsupportedOperationException(); }
        @Override default Instant now() { throw new UnsupportedOperationException(); }
        @Override default long nowMicros() { throw new UnsupportedOperationException(); }
        @Override default long nowMillis() { throw new UnsupportedOperationException(); }
    }

    interface PartialFunctionContext extends FunctionContext
    {
        default long nowMillis() { return nowMicros() / 1000; }
        default Instant now()
        {
            long nowMicros = nowMicros();
            return Instant.ofEpochSecond(nowMicros / 1000_000, (nowMicros % 1000_000) * 1000);
        }
    }

    abstract class MicrosFunctionContext implements PartialFunctionContext
    {
        final long atMicros;
        private long timeUuidNanos;

        public MicrosFunctionContext(long atMicros)
        {
            this.atMicros = atMicros;
        }

        @Override public long nowMicros() { return atMicros; }

        @Override
        public byte[] nextTimeUUIDAsBytes()
        {
            return TimeUUID.toBytes(TimeUUID.unixMicrosToMsb(atMicros), TimeUUIDType.signedBytesToNativeLong(timeUuidNanos++));
        }
    }

    QueryOptions options();
    Instant now();
    long nowMillis();
    long nowMicros();
    byte[] nextTimeUUIDAsBytes();

    default Arguments noArguments(ProtocolVersion version)
    {
        return new FunctionArguments(version, this);
    }
}
