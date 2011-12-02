/**
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

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.IRowCacheProvider;
import org.apache.cassandra.concurrent.CreationTimeAwareFuture;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.PropertyFileSnitch;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class FBUtilities
{
    private static Logger logger_ = LoggerFactory.getLogger(FBUtilities.class);

    public static final BigInteger TWO = new BigInteger("2");

    private static volatile InetAddress localInetAddress_;
    private static volatile InetAddress broadcastInetAddress_;

    private static final ThreadLocal<MessageDigest> localMD5Digest = new ThreadLocal<MessageDigest>()
    {
        @Override
        protected MessageDigest initialValue()
        {
            return newMessageDigest("MD5");
        }

        @Override
        public MessageDigest get()
        {
            MessageDigest digest = super.get();
            digest.reset();
            return digest;
        }
    };

    private static final ThreadLocal<Random> localRandom = new ThreadLocal<Random>()
    {
        @Override
        protected Random initialValue()
        {
            return new Random();
        }
    };

    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;

    public static MessageDigest threadLocalMD5Digest()
    {
        return localMD5Digest.get();
    }

    public static MessageDigest newMessageDigest(String algorithm)
    {
        try
        {
            return MessageDigest.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new RuntimeException("the requested digest algorithm (" + algorithm + ") is not available", nsae);
        }
    }

    public static Random threadLocalRandom()
    {
        return localRandom.get();
    }

    /**
     * Parses a string representing either a fraction, absolute value or percentage.
     */
    public static double parseDoubleOrPercent(String value)
    {
        if (value.endsWith("%"))
        {
            return Double.parseDouble(value.substring(0, value.length() - 1)) / 100;
        }
        else
        {
            return Double.parseDouble(value);
        }
    }

    /**
     * Please use getBroadcastAddress instead. You need this only when you have to listen/connect.
     */
    public static InetAddress getLocalAddress()
    {
        if (localInetAddress_ == null)
            try
            {
                localInetAddress_ = DatabaseDescriptor.getListenAddress() == null
                                    ? InetAddress.getLocalHost()
                                    : DatabaseDescriptor.getListenAddress();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        return localInetAddress_;
    }

    public static InetAddress getBroadcastAddress()
    {
        if (broadcastInetAddress_ == null)
            broadcastInetAddress_ = DatabaseDescriptor.getBroadcastAddress() == null
                                ? getLocalAddress()
                                : DatabaseDescriptor.getBroadcastAddress();
        return broadcastInetAddress_;
    }

    /**
     * @param fractOrAbs A double that may represent a fraction or absolute value.
     * @param total If fractionOrAbs is a fraction, the total to take the fraction from
     * @return An absolute value which may be larger than the total.
     */
    public static long absoluteFromFraction(double fractOrAbs, long total)
    {
        if (fractOrAbs < 0)
            throw new UnsupportedOperationException("unexpected negative value " + fractOrAbs);

        if (0 < fractOrAbs && fractOrAbs <= 1)
        {
            // fraction
            return Math.max(1, (long)(fractOrAbs * total));
        }

        // absolute
        assert fractOrAbs >= 1 || fractOrAbs == 0;
        return (long)fractOrAbs;
    }

    /**
     * Given two bit arrays represented as BigIntegers, containing the given
     * number of significant bits, calculate a midpoint.
     *
     * @param left The left point.
     * @param right The right point.
     * @param sigbits The number of bits in the points that are significant.
     * @return A midpoint that will compare bitwise halfway between the params, and
     * a boolean representing whether a non-zero lsbit remainder was generated.
     */
    public static Pair<BigInteger,Boolean> midpoint(BigInteger left, BigInteger right, int sigbits)
    {
        BigInteger midpoint;
        boolean remainder;
        if (left.compareTo(right) < 0)
        {
            BigInteger sum = left.add(right);
            remainder = sum.testBit(0);
            midpoint = sum.shiftRight(1);
        }
        else
        {
            BigInteger max = TWO.pow(sigbits);
            // wrapping case
            BigInteger distance = max.add(right).subtract(left);
            remainder = distance.testBit(0);
            midpoint = distance.shiftRight(1).add(left).mod(max);
        }
        return new Pair<BigInteger, Boolean>(midpoint, remainder);
    }

    /**
     * Copy bytes from int into bytes starting from offset.
     * @param bytes Target array
     * @param offset Offset into the array
     * @param i Value to write
     */
    public static void copyIntoBytes(byte[] bytes, int offset, int i)
    {
        bytes[offset]   = (byte)( ( i >>> 24 ) & 0xFF );
        bytes[offset+1] = (byte)( ( i >>> 16 ) & 0xFF );
        bytes[offset+2] = (byte)( ( i >>> 8  ) & 0xFF );
        bytes[offset+3] = (byte)(   i          & 0xFF );
    }

    /**
     * @param i Write this int to an array
     * @return 4-byte array containing the int
     */
    public static byte[] toByteArray(int i)
    {
        byte[] bytes = new byte[4];
        copyIntoBytes(bytes, 0, i);
        return bytes;
    }

    /**
     * Copy bytes from long into bytes starting from offset.
     * @param bytes Target array
     * @param offset Offset into the array
     * @param l Value to write
     */
    public static void copyIntoBytes(byte[] bytes, int offset, long l)
    {
        bytes[offset]   = (byte)( ( l >>> 56 ) & 0xFF );
        bytes[offset+1] = (byte)( ( l >>> 48 ) & 0xFF );
        bytes[offset+2] = (byte)( ( l >>> 40 ) & 0xFF );
        bytes[offset+3] = (byte)( ( l >>> 32 ) & 0xFF );
        bytes[offset+4] = (byte)( ( l >>> 24 ) & 0xFF );
        bytes[offset+5] = (byte)( ( l >>> 16 ) & 0xFF );
        bytes[offset+6] = (byte)( ( l >>> 8  ) & 0xFF );
        bytes[offset+7] = (byte)(   l          & 0xFF );
    }

    /**
     * @param l Write this long to an array
     * @return 8-byte array containing the long
     */
    public static byte[] toByteArray(long l)
    {
        byte[] bytes = new byte[8];
        copyIntoBytes(bytes, 0, l);
        return bytes;
    }

    /**
     * Convert the byte array to an int starting from the given offset.
     *
     * @param b The byte array
     *
     * @return The integer
     */
    public static int byteArrayToInt(byte[] b)
    {
        int value = 0;

        for (int i = 0; i < 4; i++)
        {
            int shift = (4 - 1 - i) * 8;
            value += (b[i] & 0x000000FF) << shift;
        }

        return value;
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2, int offset1, int offset2, int len1, int len2)
    {
        if (bytes1 == null)
        {
            return bytes2 == null ? 0 : -1;
        }
        if (bytes2 == null) return 1;

        int minLength = Math.min(len1, len2);
        for (int x = 0, i = offset1, j = offset2; x < minLength; x++, i++, j++)
        {
            if (bytes1[i] == bytes2[j])
                continue;
            // compare non-equal bytes as unsigned
            return (bytes1[i] & 0xFF) < (bytes2[j] & 0xFF) ? -1 : 1;
        }
        if (len1 == len2) return 0;
        else return (len1 < len2) ? -1 : 1;
    }
  
    /**
     * @return The bitwise XOR of the inputs. The output will be the same length as the
     * longer input, but if either input is null, the output will be null.
     */
    public static byte[] xor(byte[] left, byte[] right)
    {
        if (left == null || right == null)
            return null;
        if (left.length > right.length)
        {
            byte[] swap = left;
            left = right;
            right = swap;
        }

        // left.length is now <= right.length
        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++)
        {
            out[i] = (byte)((left[i] & 0xFF) ^ (right[i] & 0xFF));
        }
        return out;
    }

    public static BigInteger hashToBigInteger(ByteBuffer data)
    {
        byte[] result = hash(data);
        BigInteger hash = new BigInteger(result);
        return hash.abs();        
    }

    public static byte[] hash(ByteBuffer... data)
    {
        MessageDigest messageDigest = localMD5Digest.get();
        for(ByteBuffer block : data)
        {
            messageDigest.update(block.duplicate());
        }

        return messageDigest.digest();
    }

    public static void renameWithConfirm(String tmpFilename, String filename) throws IOException
    {
        if (!new File(tmpFilename).renameTo(new File(filename)))
        {
            throw new IOException("rename failed of " + filename);
        }
    }

    public static void atomicSetMax(AtomicInteger atomic, int i)
    {
        while (true)
        {
            int j = atomic.get();
            if (j >= i || atomic.compareAndSet(j, i))
                break;
        }
    }

    public static void atomicSetMax(AtomicLong atomic, long i)
    {
        while (true)
        {
            long j = atomic.get();
            if (j >= i || atomic.compareAndSet(j, i))
                break;
        }
    }

    public static void serialize(TSerializer serializer, TBase struct, DataOutput out)
    throws IOException
    {
        assert serializer != null;
        assert struct != null;
        assert out != null;
        byte[] bytes;
        try
        {
            bytes = serializer.serialize(struct);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void deserialize(TDeserializer deserializer, TBase struct, DataInput in)
    throws IOException
    {
        assert deserializer != null;
        assert struct != null;
        assert in != null;
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        try
        {
            deserializer.deserialize(struct, bytes);
        }
        catch (TException ex)
        {
            throw new IOException(ex);
        }
    }

    public static void sortSampledKeys(List<DecoratedKey> keys, Range range)
    {
        if (range.left.compareTo(range.right) >= 0)
        {
            // range wraps.  have to be careful that we sort in the same order as the range to find the right midpoint.
            final Token right = range.right;
            Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
            {
                public int compare(DecoratedKey o1, DecoratedKey o2)
                {
                    if ((right.compareTo(o1.token) < 0 && right.compareTo(o2.token) < 0)
                        || (right.compareTo(o1.token) > 0 && right.compareTo(o2.token) > 0))
                    {
                        // both tokens are on the same side of the wrap point
                        return o1.compareTo(o2);
                    }
                    return -o1.compareTo(o2);
                }
            };
            Collections.sort(keys, comparator);
        }
        else
        {
            // unwrapped range (left < right).  standard sort is all we need.
            Collections.sort(keys);
        }
    }

    public static int encodedUTF8Length(String st)
    {
        int strlen = st.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++)
        {
            int c = st.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }

    public static String resourceToFile(String filename) throws ConfigurationException
    {
        ClassLoader loader = PropertyFileSnitch.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return scpurl.getFile();
    }

    public static String getReleaseVersionString()
    {
        try
        {
            InputStream in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties");
            if (in == null)
            {
                return "Unknown";
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("CassandraVersion");
        }
        catch (Exception e)
        {
            logger_.warn("Unable to load version.properties", e);
            return "debug version";
        }
    }

    public static long timestampMicros()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }

    public static void waitOnFutures(Iterable<Future<?>> futures)
    {
        for (Future f : futures)
        {
            try
            {
                f.get();
            }
            catch (ExecutionException ee)
            {
                throw new RuntimeException(ee);
            }
            catch (InterruptedException ie)
            {
                throw new AssertionError(ie);
            }
        }
    }

    public static void waitOnFutures(List<IAsyncResult> results, long ms) throws TimeoutException
    {
        for (IAsyncResult result : results)
            result.get(ms, TimeUnit.MILLISECONDS);
    }


    /**
     * Waits for the futures to complete.
     * @param timeout the timeout expressed in <code>TimeUnit</code> units
     * @param timeUnit TimeUnit
     * @throws TimeoutException if the waiting time exceeds <code>timeout</code>
     */
    public static void waitOnFutures(List<CreationTimeAwareFuture<?>> hintFutures, long timeout, TimeUnit timeUnit) throws TimeoutException
    {
        for (Future<?> future : hintFutures)
        {
            try
            {
                future.get(timeout, timeUnit);
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError(ex);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            catch (TimeoutException e)
            {
               throw e;
            }
        }
    }

    public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException
    {
        if (!partitionerClassName.contains("."))
            partitionerClassName = "org.apache.cassandra.dht." + partitionerClassName;
        return FBUtilities.construct(partitionerClassName, "partitioner");
    }

    /**
     * @return The Class for the given name.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> Class<T> classForName(String classname, String readable) throws ConfigurationException
    {
        try
        {
            return (Class<T>)Class.forName(classname);
        }
        catch (ClassNotFoundException e)
        {
            throw new ConfigurationException(String.format("Unable to find %s class '%s'", readable, classname));
        }
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T construct(String classname, String readable) throws ConfigurationException
    {
        Class<T> cls = FBUtilities.classForName(classname, readable);
        try
        {
            return cls.getConstructor().newInstance();
        }
        catch (NoSuchMethodException e)
        {
            throw new ConfigurationException(String.format("No default constructor for %s class '%s'.", readable, classname));
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException(String.format("Default constructor for %s class '%s' is inaccessible.", readable, classname));
        }
        catch (InstantiationException e)
        {
            throw new ConfigurationException(String.format("Cannot use abstract class '%s' as %s.", classname, readable));
        }
        catch (InvocationTargetException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException)e.getCause();
            throw new ConfigurationException(String.format("Error instantiating %s class '%s'.", readable, classname), e);
        }
    }

    public static <T extends Comparable> SortedSet<T> singleton(T column)
    {
        return new TreeSet<T>(Arrays.asList(column));
    }

    public static String toString(Map<?,?> map)
    {
        Joiner.MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator(":");
        return joiner.join(map);
    }

    /**
     * Used to get access to protected/private field of the specified class
     * @param klass - name of the class
     * @param fieldName - name of the field
     * @return Field or null on error
     */
    public static Field getProtectedField(Class klass, String fieldName)
    {
        Field field;

        try
        {
            field = klass.getDeclaredField(fieldName);
            field.setAccessible(true);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }

        return field;
    }

    public static IRowCacheProvider newCacheProvider(String cache_provider) throws ConfigurationException
    {
        if (!cache_provider.contains("."))
            cache_provider = "org.apache.cassandra.cache." + cache_provider;
        return FBUtilities.construct(cache_provider, "row cache provider");
    }

    public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator)
    {
        return new WrappedCloseableIterator<T>(iterator);
    }

    private static final class WrappedCloseableIterator<T>
        extends AbstractIterator<T> implements CloseableIterator<T>
    {
        private final Iterator<T> source;
        public WrappedCloseableIterator(Iterator<T> source)
        {
            this.source = source;
        }

        protected T computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            return source.next();
        }

        public void close() {}
    }

    public static <T> byte[] serialize(T object, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        int size = (int) serializer.serializedSize(object, version);
        DataOutputBuffer buffer = new DataOutputBuffer(size);
        serializer.serialize(object, buffer, version);
        assert buffer.getLength() == size && buffer.getData().length == size
               : String.format("Final buffer length %s to accomodate data size of %s (predicted %s) for %s",
                               buffer.getData().length, buffer.getLength(), size, object);
        return buffer.getData();
    }

    public static RuntimeException unchecked(Exception e)
    {
        return e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
}
