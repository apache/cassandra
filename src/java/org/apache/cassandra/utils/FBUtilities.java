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

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
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
import java.util.zip.Checksum;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.IAllocator;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class FBUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(FBUtilities.class);

    private static ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    public static final BigInteger TWO = new BigInteger("2");
    private static final String DEFAULT_TRIGGER_DIR = "triggers";

    private static volatile InetAddress localInetAddress;
    private static volatile InetAddress broadcastInetAddress;

    private static final boolean isWindows = System.getProperty("os.name").startsWith("Windows");

    public static int getAvailableProcessors()
    {
        if (System.getProperty("cassandra.available_processors") != null)
            return Integer.parseInt(System.getProperty("cassandra.available_processors"));
        else
            return Runtime.getRuntime().availableProcessors();
    }

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
     * Please use getBroadcastAddress instead. You need this only when you have to listen/connect.
     */
    public static InetAddress getLocalAddress()
    {
        if (localInetAddress == null)
            try
            {
                localInetAddress = DatabaseDescriptor.getListenAddress() == null
                                    ? InetAddress.getLocalHost()
                                    : DatabaseDescriptor.getListenAddress();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        return localInetAddress;
    }

    public static InetAddress getBroadcastAddress()
    {
        if (broadcastInetAddress == null)
            broadcastInetAddress = DatabaseDescriptor.getBroadcastAddress() == null
                                 ? getLocalAddress()
                                 : DatabaseDescriptor.getBroadcastAddress();
        return broadcastInetAddress;
    }

    public static Collection<InetAddress> getAllLocalAddresses()
    {
        Set<InetAddress> localAddresses = new HashSet<InetAddress>();
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            if (nets != null)
            {
                while (nets.hasMoreElements())
                    localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
            }
        }
        catch (SocketException e)
        {
            throw new AssertionError(e);
        }
        return localAddresses;
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
        return Pair.create(midpoint, remainder);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2, int offset1, int offset2, int len1, int len2)
    {
        return FastByteOperations.compareUnsigned(bytes1, offset1, len1, bytes2, offset2, len2);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2)
    {
        return compareUnsigned(bytes1, bytes2, 0, 0, bytes1.length, bytes2.length);
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

    public static byte[] hash(ByteBuffer... data)
    {
        MessageDigest messageDigest = localMD5Digest.get();
        for (ByteBuffer block : data)
        {
            if (block.hasArray())
                messageDigest.update(block.array(), block.arrayOffset() + block.position(), block.remaining());
            else
                messageDigest.update(block.duplicate());
        }

        return messageDigest.digest();
    }

    public static BigInteger hashToBigInteger(ByteBuffer data)
    {
        return new BigInteger(hash(data)).abs();
    }

    @Deprecated
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

    @Deprecated
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

    public static void sortSampledKeys(List<DecoratedKey> keys, Range<Token> range)
    {
        if (range.left.compareTo(range.right) >= 0)
        {
            // range wraps.  have to be careful that we sort in the same order as the range to find the right midpoint.
            final Token right = range.right;
            Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
            {
                public int compare(DecoratedKey o1, DecoratedKey o2)
                {
                    if ((right.compareTo(o1.getToken()) < 0 && right.compareTo(o2.getToken()) < 0)
                        || (right.compareTo(o1.getToken()) > 0 && right.compareTo(o2.getToken()) > 0))
                    {
                        // both tokens are on the same side of the wrap point
                        return o1.compareTo(o2);
                    }
                    return o2.compareTo(o1);
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

    public static String resourceToFile(String filename) throws ConfigurationException
    {
        ClassLoader loader = FBUtilities.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return new File(scpurl.getFile()).getAbsolutePath();
    }

    public static File cassandraTriggerDir()
    {
        File triggerDir = null;
        if (System.getProperty("cassandra.triggers_dir") != null)
        {
            triggerDir = new File(System.getProperty("cassandra.triggers_dir"));
        }
        else
        {
            URL confDir = FBUtilities.class.getClassLoader().getResource(DEFAULT_TRIGGER_DIR);
            if (confDir != null)
                triggerDir = new File(confDir.getFile());
        }
        if (triggerDir == null || !triggerDir.exists())
        {
            logger.warn("Trigger directory doesn't exist, please create it and try again.");
            return null;
        }
        return triggerDir;
    }

    public static String getReleaseVersionString()
    {
        InputStream in = null;
        try
        {
            in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties");
            if (in == null)
            {
                return System.getProperty("cassandra.releaseVersion", "Unknown");
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("CassandraVersion");
        }
        catch (Exception e)
        {
            logger.warn("Unable to load version.properties", e);
            return "debug version";
        }
        finally
        {
            FileUtils.closeQuietly(in);
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
            waitOnFuture(f);
    }

    public static <T> T waitOnFuture(Future<T> future)
    {
        try
        {
            return future.get();
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

    public static void waitOnFutures(List<AsyncOneResponse> results, long ms) throws TimeoutException
    {
        for (AsyncOneResponse result : results)
            result.get(ms, TimeUnit.MILLISECONDS);
    }

    public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException
    {
        if (!partitionerClassName.contains("."))
            partitionerClassName = "org.apache.cassandra.dht." + partitionerClassName;
        return FBUtilities.construct(partitionerClassName, "partitioner");
    }

    public static IAllocator newOffHeapAllocator(String offheap_allocator) throws ConfigurationException
    {
        if (!offheap_allocator.contains("."))
            offheap_allocator = "org.apache.cassandra.io.util." + offheap_allocator;
        return FBUtilities.construct(offheap_allocator, "off-heap allocator");
    }

    public static IAuthorizer newAuthorizer(String className) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        return FBUtilities.construct(className, "authorizer");
    }

    public static IAuthenticator newAuthenticator(String className) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        return FBUtilities.construct(className, "authenticator");
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
            throw new ConfigurationException(String.format("Unable to find %s class '%s'", readable, classname), e);
        }
        catch (NoClassDefFoundError e)
        {
            throw new ConfigurationException(String.format("Unable to find %s class '%s'", readable, classname), e);
        }
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg or default constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T construct(String classname, String readable) throws ConfigurationException
    {
        Class<T> cls = FBUtilities.classForName(classname, readable);
        try
        {
            return cls.newInstance();
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException(String.format("Default constructor for %s class '%s' is inaccessible.", readable, classname));
        }
        catch (InstantiationException e)
        {
            throw new ConfigurationException(String.format("Cannot use abstract class '%s' as %s.", classname, readable));
        }
        catch (Exception e)
        {
            // Catch-all because Class.newInstance() "propagates any exception thrown by the nullary constructor, including a checked exception".
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException)e.getCause();
            throw new ConfigurationException(String.format("Error instantiating %s class '%s'.", readable, classname), e);
        }
    }

    public static <T> SortedSet<T> singleton(T column, Comparator<? super T> comparator)
    {
        SortedSet<T> s = new TreeSet<T>(comparator);
        s.add(column);
        return s;
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

    public static <T> CloseableIterator<T> closeableIterator(Iterator<T> iterator)
    {
        return new WrappedCloseableIterator<T>(iterator);
    }

    public static Map<String, String> fromJsonMap(String json)
    {
        try
        {
            return jsonMapper.readValue(json, Map.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static List<String> fromJsonList(String json)
    {
        try
        {
            return jsonMapper.readValue(json, List.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String json(Object object)
    {
        try
        {
            return jsonMapper.writeValueAsString(object);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts and waits for the given @param pb to finish.
     * @throws java.io.IOException on non-zero exit code
     */
    public static void exec(ProcessBuilder pb) throws IOException
    {
        Process p = pb.start();
        try
        {
            int errCode = p.waitFor();
            if (errCode != 0)
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                StringBuilder sb = new StringBuilder();
                String str;
                while ((str = in.readLine()) != null)
                    sb.append(str).append(System.getProperty("line.separator"));
                while ((str = err.readLine()) != null)
                    sb.append(str).append(System.getProperty("line.separator"));
                throw new IOException("Exception while executing the command: "+ StringUtils.join(pb.command(), " ") +
                                      ", command error Code: " + errCode +
                                      ", command output: "+ sb.toString());
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    public static void updateChecksumInt(Checksum checksum, int v)
    {
        checksum.update((v >>> 24) & 0xFF);
        checksum.update((v >>> 16) & 0xFF);
        checksum.update((v >>> 8) & 0xFF);
        checksum.update((v >>> 0) & 0xFF);
    }

    public static long abs(long index)
    {
        long negbit = index >> 63;
        return (index ^ negbit) - negbit;
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

    public static <T> byte[] serialize(T object, IVersionedSerializer<T> serializer, int version)
    {
        try
        {
            int size = (int) serializer.serializedSize(object, version);
            DataOutputBuffer buffer = new DataOutputBuffer(size);
            serializer.serialize(object, buffer, version);
            assert buffer.getLength() == size && buffer.getData().length == size
                : String.format("Final buffer length %s to accommodate data size of %s (predicted %s) for %s",
                        buffer.getData().length, buffer.getLength(), size, object);
            return buffer.getData();
        }
        catch (IOException e)
        {
            // We're doing in-memory serialization...
            throw new AssertionError(e);
        }
    }

    public static long copy(InputStream from, OutputStream to, long limit) throws IOException
    {
        byte[] buffer = new byte[64]; // 64 byte buffer
        long copied = 0;
        int toCopy = buffer.length;
        while (true)
        {
            if (limit < buffer.length + copied)
                toCopy = (int) (limit - copied);
            int sofar = from.read(buffer, 0, toCopy);
            if (sofar == -1)
                break;
            to.write(buffer, 0, sofar);
            copied += sofar;
            if (limit == copied)
                break;
        }
        return copied;
    }

    public static File getToolsOutputDirectory()
    {
        File historyDir = new File(System.getProperty("user.home"), ".cassandra");
        FileUtils.createDirectory(historyDir);
        return historyDir;
    }

    public static boolean isUnix()
    {
        return !isWindows;
    }

    public static void updateWithShort(MessageDigest digest, int val)
    {
        digest.update((byte) ((val >> 8) & 0xFF));
        digest.update((byte) (val & 0xFF));
    }

    public static void updateWithByte(MessageDigest digest, int val)
    {
        digest.update((byte) (val & 0xFF));
    }

    public static void updateWithInt(MessageDigest digest, int val)
    {
        digest.update((byte) ((val >>> 24) & 0xFF));
        digest.update((byte) ((val >>> 16) & 0xFF));
        digest.update((byte) ((val >>>  8) & 0xFF));
        digest.update((byte) ((val >>> 0) & 0xFF));
    }

    public static void updateWithLong(MessageDigest digest, long val)
    {
        digest.update((byte) ((val >>> 56) & 0xFF));
        digest.update((byte) ((val >>> 48) & 0xFF));
        digest.update((byte) ((val >>> 40) & 0xFF));
        digest.update((byte) ((val >>> 32) & 0xFF));
        digest.update((byte) ((val >>> 24) & 0xFF));
        digest.update((byte) ((val >>> 16) & 0xFF));
        digest.update((byte) ((val >>>  8) & 0xFF));
        digest.update((byte)  ((val >>> 0) & 0xFF));
    }
}
