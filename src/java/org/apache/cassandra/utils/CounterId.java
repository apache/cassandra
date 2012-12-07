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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Objects;

import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.SystemTable;

public class CounterId implements Comparable<CounterId>
{
    private static final Logger logger = LoggerFactory.getLogger(CounterId.class);

    public static final int LENGTH = 16; // we assume a fixed length size for all CounterIds

    // Lazy holder because this opens the system table and we want to avoid
    // having this triggered during class initialization
    private static class LocalIds
    {
        static final LocalCounterIdHistory instance = new LocalCounterIdHistory();
    }

    private final ByteBuffer id;

    private static LocalCounterIdHistory localIds()
    {
        return LocalIds.instance;
    }

    public static CounterId getLocalId()
    {
        return localIds().current.get();
    }

    /**
     * Renew the local counter id.
     * To use only when this strictly necessary, as using this will make all
     * counter context grow with time.
     */
    public static void renewLocalId()
    {
        renewLocalId(FBUtilities.timestampMicros());
    }

    public static synchronized void renewLocalId(long now)
    {
        localIds().renewCurrent(now);
    }

    /**
     * Return the list of old local counter id of this node.
     * It is guaranteed that the returned list is sorted by growing counter id
     * (and hence the first item will be the oldest counter id for this host)
     */
    public static List<CounterIdRecord> getOldLocalCounterIds()
    {
        return localIds().olds;
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

    public static class OneShotRenewer
    {
        private boolean renewed;
        private final CounterId initialId;

        public OneShotRenewer()
        {
            renewed = false;
            initialId = getLocalId();
        }

        public void maybeRenew(CounterColumn column)
        {
            if (!renewed && column.hasCounterId(initialId))
            {
                renewLocalId();
                renewed = true;
            }
        }
    }

    private static class LocalCounterIdHistory
    {
        private final AtomicReference<CounterId> current;
        private final List<CounterIdRecord> olds;

        LocalCounterIdHistory()
        {
            CounterId id = SystemTable.getCurrentLocalCounterId();
            if (id == null)
            {
                // no recorded local counter id, generating a new one and saving it
                id = generate();
                logger.info("No saved local counter id, using newly generated: {}", id);
                SystemTable.writeCurrentLocalCounterId(null, id, FBUtilities.timestampMicros());
                current = new AtomicReference<CounterId>(id);
                olds = new CopyOnWriteArrayList<CounterIdRecord>();
            }
            else
            {
                logger.info("Saved local counter id: {}", id);
                current = new AtomicReference<CounterId>(id);
                olds = new CopyOnWriteArrayList<CounterIdRecord>(SystemTable.getOldLocalCounterIds());
            }
        }

        synchronized void renewCurrent(long now)
        {
            CounterId newCounterId = generate();
            CounterId old = current.get();
            SystemTable.writeCurrentLocalCounterId(old, newCounterId, now);
            current.set(newCounterId);
            olds.add(new CounterIdRecord(old, now));
        }
    }

    public static class CounterIdRecord
    {
        public final CounterId id;
        public final long timestamp;

        public CounterIdRecord(CounterId id, long timestamp)
        {
            this.id = id;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CounterIdRecord otherRecord = (CounterIdRecord)o;
            return id.equals(otherRecord.id) && timestamp == otherRecord.timestamp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(id, timestamp);
        }

        public String toString()
        {
            return String.format("(%s, %d)", id.toString(), timestamp);
        }
    }
}
