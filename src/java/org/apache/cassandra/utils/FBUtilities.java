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
import java.net.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncOneResponse;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class FBUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(FBUtilities.class);

    private static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    public static final String UNKNOWN_RELEASE_VERSION = "Unknown";

    public static final BigInteger TWO = new BigInteger("2");
    private static final String DEFAULT_TRIGGER_DIR = "triggers";

    private static final String OPERATING_SYSTEM = System.getProperty("os.name").toLowerCase();
    private static final boolean IS_WINDOWS = OPERATING_SYSTEM.contains("windows");
    private static final boolean HAS_PROCFS = !IS_WINDOWS && (new File(File.separator + "proc")).exists();

    private static volatile InetAddress localInetAddress;
    private static volatile InetAddress broadcastInetAddress;
    private static volatile InetAddress broadcastRpcAddress;

    public static int getAvailableProcessors()
    {
        String availableProcessors = System.getProperty("cassandra.available_processors");
        if (!Strings.isNullOrEmpty(availableProcessors))
            return Integer.parseInt(availableProcessors);
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

    /**
     * <b>THIS IS FOR TESTING ONLY!!</b>
     */
    @VisibleForTesting
    public static void setBroadcastInetAddress(InetAddress addr)
    {
        broadcastInetAddress = addr;
    }

    public static InetAddress getBroadcastRpcAddress()
    {
        if (broadcastRpcAddress == null)
            broadcastRpcAddress = DatabaseDescriptor.getBroadcastRpcAddress() == null
                                   ? DatabaseDescriptor.getRpcAddress()
                                   : DatabaseDescriptor.getBroadcastRpcAddress();
        return broadcastRpcAddress;
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

    public static String getNetworkInterface(InetAddress localAddress)
    {
        try {
            for(NetworkInterface ifc : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if(ifc.isUp()) {
                    for(InetAddress addr : Collections.list(ifc.getInetAddresses())) {
                        if (addr.equals(localAddress))
                            return ifc.getDisplayName();
                    }
                }
            }
        }
        catch (SocketException e) {}
        return null;
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
        try (InputStream in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties"))
        {
            if (in == null)
            {
                return System.getProperty("cassandra.releaseVersion", UNKNOWN_RELEASE_VERSION);
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("CassandraVersion");
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Unable to load version.properties", e);
            return "debug version";
        }
    }

    public static String getReleaseVersionMajor()
    {
        String releaseVersion = FBUtilities.getReleaseVersionString();
        if (FBUtilities.UNKNOWN_RELEASE_VERSION.equals(releaseVersion))
        {
            throw new AssertionError("Release version is unknown");
        }
        return releaseVersion.substring(0, releaseVersion.indexOf('.'));
    }

    public static long timestampMicros()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }

    public static int nowInSeconds()
    {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static <T> List<T> waitOnFutures(Iterable<? extends Future<? extends T>> futures)
    {
        return waitOnFutures(futures, -1, null);
    }

    /**
     * Block for a collection of futures, with optional timeout.
     *
     * @param futures
     * @param timeout The number of units to wait in total. If this value is less than or equal to zero,
     *           no tiemout value will be passed to {@link Future#get()}.
     * @param units The units of timeout.
     */
    public static <T> List<T> waitOnFutures(Iterable<? extends Future<? extends T>> futures, long timeout, TimeUnit units)
    {
        long endNanos = 0;
        if (timeout > 0)
            endNanos = System.nanoTime() + units.toNanos(timeout);
        List<T> results = new ArrayList<>();
        Throwable fail = null;
        for (Future<? extends T> f : futures)
        {
            try
            {
                if (endNanos == 0)
                {
                    results.add(f.get());
                }
                else
                {
                    long waitFor = Math.max(1, endNanos - System.nanoTime());
                    results.add(f.get(waitFor, TimeUnit.NANOSECONDS));
                }
            }
            catch (Throwable t)
            {
                fail = Throwables.merge(fail, t);
            }
        }
        Throwables.maybeFail(fail);
        return results;
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

    public static void waitOnFutures(List<AsyncOneResponse<?>> results, long ms) throws TimeoutException
    {
        for (AsyncOneResponse<?> result : results)
            result.get(ms, TimeUnit.MILLISECONDS);
    }

    public static <T> Future<? extends T> waitOnFirstFuture(Iterable<? extends Future<? extends T>> futures)
    {
        return waitOnFirstFuture(futures, 100);
    }
    /**
     * Only wait for the first future to finish from a list of futures. Will block until at least 1 future finishes.
     * @param futures The futures to wait on
     * @return future that completed.
     */
    public static <T> Future<? extends T> waitOnFirstFuture(Iterable<? extends Future<? extends T>> futures, long delay)
    {
        while (true)
        {
            for (Future<? extends T> f : futures)
            {
                if (f.isDone())
                {
                    try
                    {
                        f.get();
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                    catch (ExecutionException e)
                    {
                        throw new RuntimeException(e);
                    }
                    return f;
                }
            }
            Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
        }
    }
    /**
     * Create a new instance of a partitioner defined in an SSTable Descriptor
     * @param desc Descriptor of an sstable
     * @return a new IPartitioner instance
     * @throws IOException
     */
    public static IPartitioner newPartitioner(Descriptor desc) throws IOException
    {
        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        if (validationMetadata.partitioner.endsWith("LocalPartitioner"))
        {
            return new LocalPartitioner(header.getKeyType());
        }
        else
        {
            return newPartitioner(validationMetadata.partitioner);
        }
    }

    public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException
    {
        if (!partitionerClassName.contains("."))
            partitionerClassName = "org.apache.cassandra.dht." + partitionerClassName;
        return FBUtilities.instanceOrConstruct(partitionerClassName, "partitioner");
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

    public static IRoleManager newRoleManager(String className) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        return FBUtilities.construct(className, "role manager");
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
        catch (ClassNotFoundException | NoClassDefFoundError e)
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
    public static <T> T instanceOrConstruct(String classname, String readable) throws ConfigurationException
    {
        Class<T> cls = FBUtilities.classForName(classname, readable);
        try
        {
            Field instance = cls.getField("instance");
            return cls.cast(instance.get(null));
        }
        catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e)
        {
            // Could not get instance field. Try instantiating.
            return construct(cls, classname, readable);
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
        return construct(cls, classname, readable);
    }

    private static <T> T construct(Class<T> cls, String classname, String readable) throws ConfigurationException
    {
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

    public static <T> NavigableSet<T> singleton(T column, Comparator<? super T> comparator)
    {
        NavigableSet<T> s = new TreeSet<T>(comparator);
        s.add(column);
        return s;
    }

    public static <T> NavigableSet<T> emptySortedSet(Comparator<? super T> comparator)
    {
        return new TreeSet<T>(comparator);
    }

    /**
     * Make straing out of the given {@code Map}.
     *
     * @param map Map to make string.
     * @return String representation of all entries in the map,
     *         where key and value pair is concatenated with ':'.
     */
    @Nonnull
    public static String toString(@Nullable Map<?, ?> map)
    {
        if (map == null)
            return "";
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
        try
        {
            Field field = klass.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
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

    public static String prettyPrintMemory(long size)
    {
        if (size >= 1 << 30)
            return String.format("%.3fGiB", size / (double) (1 << 30));
        if (size >= 1 << 20)
            return String.format("%.3fMiB", size / (double) (1 << 20));
        return String.format("%.3fKiB", size / (double) (1 << 10));
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
            	try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                     BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream())))
                {
            		String lineSep = System.getProperty("line.separator");
	                StringBuilder sb = new StringBuilder();
	                String str;
	                while ((str = in.readLine()) != null)
	                    sb.append(str).append(lineSep);
	                while ((str = err.readLine()) != null)
	                    sb.append(str).append(lineSep);
	                throw new IOException("Exception while executing the command: "+ StringUtils.join(pb.command(), " ") +
	                                      ", command error Code: " + errCode +
	                                      ", command output: "+ sb.toString());
                }
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

    /**
      * Updates checksum with the provided ByteBuffer at the given offset + length.
      * Resets position and limit back to their original values on return.
      * This method is *NOT* thread-safe.
      */
    public static void updateChecksum(CRC32 checksum, ByteBuffer buffer, int offset, int length)
    {
        int position = buffer.position();
        int limit = buffer.limit();

        buffer.position(offset).limit(offset + length);
        checksum.update(buffer);

        buffer.position(position).limit(limit);
    }

    /**
     * Updates checksum with the provided ByteBuffer.
     * Resets position back to its original values on return.
     * This method is *NOT* thread-safe.
     */
    public static void updateChecksum(CRC32 checksum, ByteBuffer buffer)
    {
        int position = buffer.position();
        checksum.update(buffer);
        buffer.position(position);
    }

    private static final ThreadLocal<byte[]> threadLocalScratchBuffer = new ThreadLocal<byte[]>()
    {
        @Override
        protected byte[] initialValue()
        {
            return new byte[CompressionParams.DEFAULT_CHUNK_LENGTH];
        }
    };

    public static byte[] getThreadLocalScratchBuffer()
    {
        return threadLocalScratchBuffer.get();
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
        int size = (int) serializer.serializedSize(object, version);

        try (DataOutputBuffer buffer = new DataOutputBufferFixed(size))
        {
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

    public static boolean isWindows()
    {
        return IS_WINDOWS;
    }

    public static boolean hasProcFS()
    {
        return HAS_PROCFS;
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

    public static void updateWithBoolean(MessageDigest digest, boolean val)
    {
        updateWithByte(digest, val ? 0 : 1);
    }

    public static void closeAll(Collection<? extends AutoCloseable> l) throws Exception
    {
        Exception toThrow = null;
        for (AutoCloseable c : l)
        {
            try
            {
                c.close();
            }
            catch (Exception e)
            {
                if (toThrow == null)
                    toThrow = e;
                else
                    toThrow.addSuppressed(e);
            }
        }
        if (toThrow != null)
            throw toThrow;
    }

    public static byte[] toWriteUTFBytes(String s)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF(s);
            dos.flush();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected static void reset()
    {
        localInetAddress = null;
        broadcastInetAddress = null;
        broadcastRpcAddress = null;
    }
}
