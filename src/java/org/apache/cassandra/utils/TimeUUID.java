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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_UNSAFE_TIME_UUID_NODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.DETERMINISM_UNSAFE_UUID_NODE;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;

@Shared(inner = INTERFACES)
public class TimeUUID implements Serializable, Comparable<TimeUUID>
{
    public static final long serialVersionUID = 1L;

    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    public static final long UUID_EPOCH_UNIX_MILLIS = -12219292800000L;
    protected static final long TIMESTAMP_UUID_VERSION_IN_MSB = 0x1000L;
    protected static final long UUID_VERSION_BITS_IN_MSB = 0xf000L;

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

    public static final long TIMEUUID_SIZE = ObjectSizes.measureDeep(new TimeUUID(10, 10));

    final long uuidTimestamp, lsb;

    public TimeUUID(long uuidTimestamp, long lsb)
    {
        // we don't validate that this is a true TIMEUUID to avoid problems with historical mixing of UUID with TimeUUID
        this.uuidTimestamp = uuidTimestamp;
        this.lsb = lsb;
    }

    public static TimeUUID atUnixMicrosWithLsb(long unixMicros, long uniqueLsb)
    {
        return new TimeUUID(unixMicrosToRawTimestamp(unixMicros), uniqueLsb);
    }

    public static UUID atUnixMicrosWithLsbAsUUID(long unixMicros, long uniqueLsb)
    {
        return new UUID(rawTimestampToMsb(unixMicrosToRawTimestamp(unixMicros)), uniqueLsb);
    }

    /**
     * Returns the smaller possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static TimeUUID minAtUnixMillis(long unixMillis)
    {
        return new TimeUUID(unixMillisToRawTimestamp(unixMillis, 0), MIN_CLOCK_SEQ_AND_NODE);
    }

    /**
     * Returns the biggest possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    public static TimeUUID maxAtUnixMillis(long unixMillis)
    {
        return new TimeUUID(unixMillisToRawTimestamp(unixMillis + 1, 0) - 1, MAX_CLOCK_SEQ_AND_NODE);
    }

    public static TimeUUID fromString(String uuidString)
    {
        return fromUuid(UUID.fromString(uuidString));
    }

    public static TimeUUID fromUuid(UUID uuid)
    {
        return fromBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static TimeUUID fromBytes(long msb, long lsb)
    {
        return new TimeUUID(msbToRawTimestamp(msb), lsb);
    }

    public static TimeUUID deserialize(ByteBuffer buffer)
    {
        return fromBytes(buffer.getLong(buffer.position()), buffer.getLong(buffer.position() + 8));
    }

    public static TimeUUID deserialize(DataInput in) throws IOException
    {
        long msb = in.readLong();
        long lsb = in.readLong();
        return fromBytes(msb, lsb);
    }

    public void serialize(DataOutput out) throws IOException
    {
        out.writeLong(msb());
        out.writeLong(lsb());
    }

    public ByteBuffer toBytes()
    {
        return ByteBuffer.wrap(toBytes(msb(), lsb()));
    }

    public static byte[] toBytes(long msb, long lsb)
    {
        byte[] uuidBytes = new byte[16];

        for (int i = 0; i < 8; i++)
            uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            uuidBytes[i] = (byte) (lsb >>> 8 * (15 - i));

        return uuidBytes;
    }

    public static long sizeInBytes()
    {
        return 16;
    }

    public UUID asUUID()
    {
        return new UUID(rawTimestampToMsb(uuidTimestamp), lsb);
    }

    /**
     * The Cassandra internal micros-resolution timestamp of the TimeUUID, as of unix epoch
     */
    public long unix(TimeUnit units)
    {
        return units.convert(unixMicros(), MICROSECONDS);
    }

    /**
     * The Cassandra internal micros-resolution timestamp of the TimeUUID, as of unix epoch
     */
    public long unixMicros()
    {
        return rawTimestampToUnixMicros(uuidTimestamp);
    }

    /**
     * The UUID-format timestamp, i.e. 10x micros-resolution, as of UUIDGen.UUID_EPOCH_UNIX_MILLIS
     * The tenths of a microsecond are used to store a flag value.
     */
    public long uuidTimestamp()
    {
        return uuidTimestamp & 0x0FFFFFFFFFFFFFFFL;
    }

    public long msb()
    {
        return rawTimestampToMsb(uuidTimestamp);
    }

    public long lsb()
    {
        return lsb;
    }

    public static long rawTimestampToUnixMicros(long rawTimestamp)
    {
        return (rawTimestamp / 10) + UUID_EPOCH_UNIX_MILLIS * 1000;
    }

    public static long unixMillisToRawTimestamp(long unixMillis, long tenthsOfAMicro)
    {
        return unixMillis * 10000 - (UUID_EPOCH_UNIX_MILLIS * 10000) + tenthsOfAMicro;
    }

    public static long unixMicrosToRawTimestamp(long unixMicros)
    {
        return unixMicros * 10 - (UUID_EPOCH_UNIX_MILLIS * 10000);
    }

    public static long msbToRawTimestamp(long msb)
    {
        assert (UUID_VERSION_BITS_IN_MSB & msb) == TIMESTAMP_UUID_VERSION_IN_MSB;
        msb &= ~TIMESTAMP_UUID_VERSION_IN_MSB;
        return   (msb &     0xFFFFL) << 48
               | (msb & 0xFFFF0000L) << 16
               | (msb >>> 32);
    }

    public static long rawTimestampToMsb(long rawTimestamp)
    {
        return TIMESTAMP_UUID_VERSION_IN_MSB
               | (rawTimestamp >>> 48)
               | ((rawTimestamp & 0xFFFF00000000L) >>> 16)
               | (rawTimestamp << 32);
    }

    @Override
    public int hashCode()
    {
        return (int) ((uuidTimestamp ^ (uuidTimestamp >> 32) * 31) + (lsb ^ (lsb >> 32)));
    }

    @Override
    public boolean equals(Object that)
    {
        return    (that instanceof UUID && equals((UUID) that))
               || (that instanceof TimeUUID && equals((TimeUUID) that));
    }

    public boolean equals(TimeUUID that)
    {
        return that != null && uuidTimestamp == that.uuidTimestamp && lsb == that.lsb;
    }

    public boolean equals(UUID that)
    {
        return that != null && uuidTimestamp == that.timestamp() && lsb == that.getLeastSignificantBits();
    }

    @Override
    public String toString()
    {
        return asUUID().toString();
    }

    public static String toString(TimeUUID ballot)
    {
        return ballot == null ? "null" : ballot.uuidTimestamp() + ":" + ballot;
    }

    public static String toString(TimeUUID ballot, String kind)
    {
        return ballot == null ? "null" : String.format("%s(%d:%s)", kind, ballot.uuidTimestamp(), ballot);
    }

    @Override
    public int compareTo(TimeUUID that)
    {
        return this.uuidTimestamp != that.uuidTimestamp
               ? Long.compare(this.uuidTimestamp, that.uuidTimestamp)
               : Long.compare(this.lsb, that.lsb);
    }

    protected static abstract class AbstractSerializer<T extends TimeUUID> extends TypeSerializer<T>
    {
        public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
        {
            if (accessor.isEmpty(value))
                return;

            if (accessor.size(value) != 16)
                throw new MarshalException(String.format("UUID should be 16 or 0 bytes (%d)", accessor.size(value)));

            if ((accessor.getByte(value, 6) & 0xf0) != 0x10)
                throw new MarshalException(String.format("Invalid version for TimeUUID type: 0x%s", Integer.toHexString((accessor.getByte(value, 0) >> 4) & 0xf)));
        }

        public String toString(T value)
        {
            return value == null ? "" : value.toString();
        }

        public ByteBuffer serialize(T value)
        {
            if (value == null)
                return EMPTY_BYTE_BUFFER;
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(value.msb());
            buffer.putLong(value.lsb());
            buffer.flip();
            return buffer;
        }
    }

    public static class Serializer extends AbstractSerializer<TimeUUID> implements IVersionedSerializer<TimeUUID>
    {
        public static final Serializer instance = new Serializer();

        public <V> TimeUUID deserialize(V value, ValueAccessor<V> accessor)
        {
            return accessor.isEmpty(value) ? null : accessor.toTimeUUID(value);
        }

        public Class<TimeUUID> getType()
        {
            return TimeUUID.class;
        }

        @Override
        public void serialize(TimeUUID t, DataOutputPlus out, int version) throws IOException
        {
            t.serialize(out);
        }

        @Override
        public TimeUUID deserialize(DataInputPlus in, int version) throws IOException
        {
            return TimeUUID.deserialize(in);
        }

        @Override
        public long serializedSize(TimeUUID t, int version)
        {
            return 16;
        }
    }

    public static class Generator
    {
        private static final long clockSeqAndNode = makeClockSeqAndNode();

        private static final AtomicLong lastMicros = new AtomicLong();

        public static TimeUUID nextTimeUUID()
        {
            return atUnixMicrosWithLsb(nextUnixMicros(), clockSeqAndNode);
        }

        public static UUID nextTimeAsUUID()
        {
            return atUnixMicrosWithLsbAsUUID(nextUnixMicros(), clockSeqAndNode);
        }

        public static TimeUUID atUnixMillis(long unixMillis)
        {
            return atUnixMillis(unixMillis, 0);
        }

        public static TimeUUID atUnixMillis(long unixMillis, long tenthsOfAMicro)
        {
            return new TimeUUID(unixMillisToRawTimestamp(unixMillis, tenthsOfAMicro), clockSeqAndNode);
        }

        public static byte[] atUnixMillisAsBytes(long unixMillis)
        {
            return atUnixMillisAsBytes(unixMillis, 0);
        }

        public static byte[] atUnixMillisAsBytes(long unixMillis, long tenthsOfAMicro)
        {
            return toBytes(rawTimestampToMsb(unixMillisToRawTimestamp(unixMillis, tenthsOfAMicro)), clockSeqAndNode);
        }

        public static byte[] nextTimeUUIDAsBytes()
        {
            return toBytes(rawTimestampToMsb(unixMicrosToRawTimestamp(nextUnixMicros())), clockSeqAndNode);
        }

        // needs to return two different values for the same when.
        // we can generate at most 10k UUIDs per ms.
        private static long nextUnixMicros()
        {
            long newLastMicros;
            while (true)
            {
                //Generate a candidate value for new lastNanos
                newLastMicros = currentTimeMillis() * 1000;
                long originalLastNanos = lastMicros.get();
                if (newLastMicros > originalLastNanos)
                {
                    //Slow path once per millisecond do a CAS
                    if (lastMicros.compareAndSet(originalLastNanos, newLastMicros))
                    {
                        break;
                    }
                }
                else
                {
                    //Fast path do an atomic increment
                    //Or when falling behind this will move time forward past the clock if necessary
                    newLastMicros = lastMicros.incrementAndGet();
                    break;
                }
            }
            return newLastMicros;
        }

        private static long makeClockSeqAndNode()
        {
            if (DETERMINISM_UNSAFE_UUID_NODE.getBoolean())
                return FBUtilities.getBroadcastAddressAndPort().addressBytes[3];

            if (CASSANDRA_UNSAFE_TIME_UUID_NODE.isPresent())
                return CASSANDRA_UNSAFE_TIME_UUID_NODE.getLong()
                       ^ FBUtilities.getBroadcastAddressAndPort().addressBytes[3]
                       ^ (FBUtilities.getBroadcastAddressAndPort().addressBytes[2] << 8);

            long clock = new SecureRandom().nextLong();

            long lsb = 0;
            lsb |= 0x8000000000000000L;                 // variant (2 bits)
            lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
            lsb |= makeNode();                          // 6 bytes
            return lsb;
        }

        private static long makeNode()
        {
            /*
             * We don't have access to the MAC address but need to generate a node part
             * that identify this host as uniquely as possible.
             * The spec says that one option is to take as many source that identify
             * this node as possible and hash them together. That's what we do here by
             * gathering all the ip of this host.
             * Note that FBUtilities.getJustBroadcastAddress() should be enough to uniquely
             * identify the node *in the cluster* but it triggers DatabaseDescriptor
             * instanciation and the UUID generator is used in Stress for instance,
             * where we don't want to require the yaml.
             */
            Collection<InetAddressAndPort> localAddresses = getAllLocalAddresses();
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

        private static byte[] hash(Collection<InetAddressAndPort> data)
        {
            // Identify the host.
            Hasher hasher = Hashing.md5().newHasher();
            for(InetAddressAndPort addr : data)
            {
                hasher.putBytes(addr.addressBytes);
                hasher.putInt(addr.getPort());
            }

            // Identify the process on the load: we use both the PID and class loader hash.
            long pid = NativeLibrary.getProcessID();
            if (pid < 0)
                pid = new Random(currentTimeMillis()).nextLong();
            updateWithLong(hasher, pid);

            ClassLoader loader = UUIDGen.class.getClassLoader();
            int loaderId = loader != null ? System.identityHashCode(loader) : 0;
            updateWithInt(hasher, loaderId);

            return hasher.hash().asBytes();
        }

        private static void updateWithInt(Hasher hasher, int val)
        {
            hasher.putByte((byte) ((val >>> 24) & 0xFF));
            hasher.putByte((byte) ((val >>> 16) & 0xFF));
            hasher.putByte((byte) ((val >>>  8) & 0xFF));
            hasher.putByte((byte) ((val >>> 0) & 0xFF));
        }

        public static void updateWithLong(Hasher hasher, long val)
        {
            hasher.putByte((byte) ((val >>> 56) & 0xFF));
            hasher.putByte((byte) ((val >>> 48) & 0xFF));
            hasher.putByte((byte) ((val >>> 40) & 0xFF));
            hasher.putByte((byte) ((val >>> 32) & 0xFF));
            hasher.putByte((byte) ((val >>> 24) & 0xFF));
            hasher.putByte((byte) ((val >>> 16) & 0xFF));
            hasher.putByte((byte) ((val >>>  8) & 0xFF));
            hasher.putByte((byte)  ((val >>> 0) & 0xFF));
        }

        /**
         * Helper function used exclusively by UUIDGen to create
         **/
        public static Collection<InetAddressAndPort> getAllLocalAddresses()
        {
            Set<InetAddressAndPort> localAddresses = new HashSet<>();
            try
            {
                Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
                if (nets != null)
                {
                    while (nets.hasMoreElements())
                    {
                        Function<InetAddress, InetAddressAndPort> converter =
                        address -> InetAddressAndPort.getByAddressOverrideDefaults(address, 0);
                        List<InetAddressAndPort> addresses =
                        Collections.list(nets.nextElement().getInetAddresses()).stream().map(converter).collect(Collectors.toList());
                        localAddresses.addAll(addresses);
                    }
                }
            }
            catch (SocketException e)
            {
                throw new AssertionError(e);
            }
            if (DatabaseDescriptor.isDaemonInitialized())
            {
                localAddresses.add(FBUtilities.getBroadcastAddressAndPort());
                localAddresses.add(FBUtilities.getBroadcastNativeAddressAndPort());
                localAddresses.add(FBUtilities.getLocalAddressAndPort());
            }
            return localAddresses;
        }
    }
}

// ---Copied from UUIDGen
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
