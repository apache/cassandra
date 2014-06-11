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
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.SystemKeyspace;

public class CounterId implements Comparable<CounterId>
{
    public static final int LENGTH = 16; // we assume a fixed length size for all CounterIds

    // Lazy holder because this opens the system keyspace and we want to avoid
    // having this triggered during class initialization
    private static class LocalId
    {
        static final LocalCounterIdHolder instance = new LocalCounterIdHolder();
    }

    private final ByteBuffer id;

    private static LocalCounterIdHolder localId()
    {
        return LocalId.instance;
    }

    public static CounterId getLocalId()
    {
        return localId().get();
    }

    /**
     * Function for test purposes, do not use otherwise.
     * Pack an int in a valid CounterId so that the resulting ids respects the
     * numerical ordering. Used for creating handcrafted but easy to
     * understand contexts in unit tests (see CounterContextTest).
     */
    public static CounterId fromInt(int n)
    {
        long lowBits = 0xC000000000000000L | n;
        return new CounterId(ByteBuffer.allocate(16).putLong(0, 0).putLong(8, lowBits));
    }

    /*
     * For performance reasons, this function interns the provided ByteBuffer.
     */
    public static CounterId wrap(ByteBuffer id)
    {
        return new CounterId(id);
    }

    public static CounterId wrap(ByteBuffer bb, int offset)
    {
        ByteBuffer dup = bb.duplicate();
        dup.position(offset);
        dup.limit(dup.position() + LENGTH);
        return wrap(dup);
    }

    private CounterId(ByteBuffer id)
    {
        if (id.remaining() != LENGTH)
            throw new IllegalArgumentException("A CounterId representation is exactly " + LENGTH + " bytes");

        this.id = id;
    }

    public static CounterId generate()
    {
        return new CounterId(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));
    }

    /*
     * For performance reasons, this function returns a reference to the internal ByteBuffer. Clients not modify the
     * result of this function.
     */
    public ByteBuffer bytes()
    {
        return id;
    }

    public boolean isLocalId()
    {
        return equals(getLocalId());
    }

    public int compareTo(CounterId o)
    {
        return ByteBufferUtil.compareSubArrays(id, id.position(), o.id, o.id.position(), CounterId.LENGTH);
    }

    @Override
    public String toString()
    {
        return UUIDGen.getUUID(id).toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CounterId otherId = (CounterId)o;
        return id.equals(otherId.id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    private static class LocalCounterIdHolder
    {
        private final AtomicReference<CounterId> current;

        LocalCounterIdHolder()
        {
            current = new AtomicReference<>(wrap(ByteBufferUtil.bytes(SystemKeyspace.getLocalHostId())));
        }

        CounterId get()
        {
            return current.get();
        }
    }
}
