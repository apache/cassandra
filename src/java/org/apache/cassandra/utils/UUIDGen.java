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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

/**
 * The goods are here: www.ietf.org/rfc/rfc4122.txt.
 */
public class UUIDGen
{
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    private static final long clockSeqAndNode = makeClockSeqAndNode();

    /*
     * The min and max possible lsb for a UUID.
     * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
     * compares the lsb parts as a signed byte array comparison. So the min
     * value is 8 times -128 and the max is 8 times +127.
     *
     * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
     * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
     * I don't think that has any practical consequence and is more robust in
     * case someone provides a UUID with a broken variant.
     */
    private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
    private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

    private static final SecureRandom secureRandom = new SecureRandom();

    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    private static final UUIDGen instance = new UUIDGen();

    private AtomicLong lastNanos = new AtomicLong();

    private UUIDGen()
    {
        // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
        if (clockSeqAndNode == 0) throw new RuntimeException("singleton instantiation is misplaced.");
    }

    /**
     * Creates a type 1 UUID (time-based UUID).
     *
     * @return a UUID instance
     */
    public static UUID getTimeUUID()
    {
        return new UUID(instance.createTimeSafe(), clockSeqAndNode);
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the timestamp of @param when, in milliseconds.
     *
     * @return a UUID instance
     */
    public static UUID getTimeUUID(long when)
    {
        return new UUID(createTime(fromUnixTimestamp(when)), clockSeqAndNode);
    }

    /**
     * Returns a version 1 UUID using the provided timestamp and the local clock and sequence.
     * <p>
     * Note that this method is generally only safe to use if you can guarantee that the provided
     * parameter is unique across calls (otherwise the returned UUID won't be unique accross calls).
     *
     * @param whenInMicros a unix time in microseconds.
     * @return a new UUID {@code id} such that {@code microsTimestamp(id) == whenInMicros}. Please not that
     * multiple calls to this method with the same value of {@code whenInMicros} will return the <b>same</b>
     * UUID.
     */
    public static UUID getTimeUUIDFromMicros(long whenInMicros)
    {
        long whenInMillis = whenInMicros / 1000;
        long nanos = (whenInMicros - (whenInMillis * 1000)) * 10;
        return getTimeUUID(whenInMillis, nanos);
    }

    /**
     * Similar to {@link getTimeUUIDFromMicros}, but randomize (using SecureRandom) the clock and sequence.
     * <p>
     * If you can guarantee that the {@code whenInMicros} argument is unique (for this JVM instance) for
     * every call, then you should prefer {@link getTimeUUIDFromMicros} which is faster. If you can't
     * guarantee this however, this method will ensure the returned UUID are still unique (accross calls)
     * through randomization.
     *
     * @param whenInMicros a unix time in microseconds.
     * @return a new UUID {@code id} such that {@code microsTimestamp(id) == whenInMicros}. The UUID returned
     * by different calls will be unique even if {@code whenInMicros} is not.
     */
    public static UUID getRandomTimeUUIDFromMicros(long whenInMicros)
    {
        long whenInMillis = whenInMicros / 1000;
        long nanos = (whenInMicros - (whenInMillis * 1000)) * 10;
        return new UUID(createTime(fromUnixTimestamp(whenInMillis, nanos)), secureRandom.nextLong());
    }

    public static UUID getTimeUUID(long when, long nanos)
    {
        return new UUID(createTime(fromUnixTimestamp(when, nanos)), clockSeqAndNode);
    }

    @VisibleForTesting
    public static UUID getTimeUUID(long when, long nanos, long clockSeqAndNode)
    {
        return new UUID(createTime(fromUnixTimestamp(when, nanos)), clockSeqAndNode);
    }

    /** creates a type 1 uuid from raw bytes. */
    public static UUID getUUID(ByteBuffer raw)
    {
        return new UUID(raw.getLong(raw.position()), raw.getLong(raw.position() + 8));
    }

    public static ByteBuffer toByteBuffer(UUID uuid)
    {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.flip();
        return buffer;
    }

    /** decomposes a uuid into raw bytes. */
    public static byte[] decompose(UUID uuid)
    {
        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();
        byte[] b = new byte[16];
        for (int i = 0; i < 8; i++)
        {
            b[i] = (byte)(most >>> ((7-i) * 8));
            b[8+i] = (byte)(least >>> ((7-i) * 8));
        }
        return b;
    }

    /**
     * Returns a 16 byte representation of a type 1 UUID (a time-based UUID),
     * based on the current system time.
     *
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes()
    {
        return createTimeUUIDBytes(instance.createTimeSafe());
    }

    /**
     * Returns the smaller possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static UUID minTimeUUID(long timestamp)
    {
        return new UUID(createTime(fromUnixTimestamp(timestamp)), MIN_CLOCK_SEQ_AND_NODE);
    }

    /**
     * Returns the biggest possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static UUID maxTimeUUID(long timestamp)
    {
        // unix timestamp are milliseconds precision, uuid timestamp are 100's
        // nanoseconds precision. If we ask for the biggest uuid have unix
        // timestamp 1ms, then we should not extend 100's nanoseconds
        // precision by taking 10000, but rather 19999.
        long uuidTstamp = fromUnixTimestamp(timestamp + 1) - 1;
        return new UUID(createTime(uuidTstamp), MAX_CLOCK_SEQ_AND_NODE);
    }

    /**
     * @param uuid
     * @return milliseconds since Unix epoch
     */
    public static long unixTimestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    /**
     * @param uuid
     * @return seconds since Unix epoch
     */
    public static int unixTimestampInSec(UUID uuid)
    {
        return Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(unixTimestamp(uuid)));
    }

    /**
     * @param uuid
     * @return microseconds since Unix epoch
     */
    public static long microsTimestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10) + START_EPOCH * 1000;
    }

    /**
     * @param timestamp milliseconds since Unix epoch
     * @return
     */
    private static long fromUnixTimestamp(long timestamp)
    {
        return fromUnixTimestamp(timestamp, 0L);
    }

    private static long fromUnixTimestamp(long timestamp, long nanos)
    {
        return ((timestamp - START_EPOCH) * 10000) + nanos;
    }

    /**
     * Converts a 100-nanoseconds precision timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     *
     * To specify a 100-nanoseconds precision timestamp, one should provide a milliseconds timestamp and
     * a number {@code 0 <= n < 10000} such that n*100 is the number of nanoseconds within that millisecond.
     *
     * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
     * invocations using identical timestamps will result in identical UUIDs.</i></p>
     *
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes(long timeMillis, int nanos)
    {
        if (nanos >= 10000)
            throw new IllegalArgumentException();
        return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis, nanos));
    }

    private static byte[] createTimeUUIDBytes(long msb)
    {
        long lsb = clockSeqAndNode;
        byte[] uuidBytes = new byte[16];

        for (int i = 0; i < 8; i++)
            uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            uuidBytes[i] = (byte) (lsb >>> 8 * (7 - i));

        return uuidBytes;
    }

    /**
     * Returns a milliseconds-since-epoch value for a type-1 UUID.
     *
     * @param uuid a type-1 (time-based) UUID
     * @return the number of milliseconds since the unix epoch
     * @throws IllegalArgumentException if the UUID is not version 1
     */
    public static long getAdjustedTimestamp(UUID uuid)
    {
        if (uuid.version() != 1)
            throw new IllegalArgumentException("incompatible with uuid version: "+uuid.version());
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    private static long makeClockSeqAndNode()
    {
        long clock = new SecureRandom().nextLong();

        long lsb = 0;
        lsb |= 0x8000000000000000L;                 // variant (2 bits)
        lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
        lsb |= makeNode();                          // 6 bytes
        return lsb;
    }

    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    private long createTimeSafe()
    {
        long newLastNanos;
        while (true)
        {
            //Generate a candidate value for new lastNanos
            newLastNanos = (System.currentTimeMillis() - START_EPOCH) * 10000;
            long originalLastNanos = lastNanos.get();
            if (newLastNanos > originalLastNanos)
            {
                //Slow path once per millisecond do a CAS
                if (lastNanos.compareAndSet(originalLastNanos, newLastNanos))
                {
                    break;
                }
            }
            else
            {
                //Fast path do an atomic increment
                //Or when falling behind this will move time forward past the clock if necessary
                newLastNanos = lastNanos.incrementAndGet();
                break;
            }
        }
        return createTime(newLastNanos);
    }

    private long createTimeUnsafe(long when, int nanos)
    {
        long nanosSince = ((when - START_EPOCH) * 10000) + nanos;
        return createTime(nanosSince);
    }

    private static long createTime(long nanosSince)
    {
        long msb = 0L;
        msb |= (0x00000000ffffffffL & nanosSince) << 32;
        msb |= (0x0000ffff00000000L & nanosSince) >>> 16;
        msb |= (0xffff000000000000L & nanosSince) >>> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }

    private static long makeNode()
    {
       /*
        * We don't have access to the MAC address but need to generate a node part
        * that identify this host as uniquely as possible.
        * The spec says that one option is to take as many source that identify
        * this node as possible and hash them together. That's what we do here by
        * gathering all the ip of this host.
        * Note that FBUtilities.getBroadcastAddress() should be enough to uniquely
        * identify the node *in the cluster* but it triggers DatabaseDescriptor
        * instanciation and the UUID generator is used in Stress for instance,
        * where we don't want to require the yaml.
        */
        Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();
        if (localAddresses.isEmpty())
            throw new RuntimeException("Cannot generate the node component of the UUID because cannot retrieve any IP addresses.");

        // ideally, we'd use the MAC address, but java doesn't expose that.
        byte[] hash = hash(localAddresses);
        long node = 0;
        for (int i = 0; i < Math.min(6, hash.length); i++)
            node |= (0x00000000000000ff & (long)hash[i]) << (5-i)*8;
        assert (0xff00000000000000L & node) == 0;

        // Since we don't use the mac address, the spec says that multicast
        // bit (least significant bit of the first octet of the node ID) must be 1.
        return node | 0x0000010000000000L;
    }

    private static byte[] hash(Collection<InetAddress> data)
    {
        try
        {
            // Identify the host.
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            for(InetAddress addr : data)
                messageDigest.update(addr.getAddress());

            // Identify the process on the load: we use both the PID and class loader hash.
            long pid = NativeLibrary.getProcessID();
            if (pid < 0)
                pid = new Random(System.currentTimeMillis()).nextLong();
            FBUtilities.updateWithLong(messageDigest, pid);

            ClassLoader loader = UUIDGen.class.getClassLoader();
            int loaderId = loader != null ? System.identityHashCode(loader) : 0;
            FBUtilities.updateWithInt(messageDigest, loaderId);

            return messageDigest.digest();
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new RuntimeException("MD5 digest algorithm is not available", nsae);
        }
    }
}

// for the curious, here is how I generated START_EPOCH
//        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
//        c.set(Calendar.YEAR, 1582);
//        c.set(Calendar.MONTH, Calendar.OCTOBER);
//        c.set(Calendar.DAY_OF_MONTH, 15);
//        c.set(Calendar.HOUR_OF_DAY, 0);
//        c.set(Calendar.MINUTE, 0);
//        c.set(Calendar.SECOND, 0);
//        c.set(Calendar.MILLISECOND, 0);
//        long START_EPOCH = c.getTimeInMillis();
