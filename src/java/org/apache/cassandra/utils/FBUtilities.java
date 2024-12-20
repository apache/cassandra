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


import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Suppliers;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.repair.autorepair.IAutoRepairTokenRangeSplitter;
import org.apache.cassandra.utils.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.audit.IAuditLogger;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.INetworkAuthorizer;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
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
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.config.CassandraRelevantProperties.LINE_SEPARATOR;
import static org.apache.cassandra.config.CassandraRelevantProperties.USER_HOME;
import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;


public class FBUtilities
{
    private static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    static
    {
        preventIllegalAccessWarnings();
        jsonMapper.registerModule(new JavaTimeModule());
        jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private static final Logger logger = LoggerFactory.getLogger(FBUtilities.class);
    public static final String UNKNOWN_RELEASE_VERSION = "Unknown";

    public static final BigInteger TWO = new BigInteger("2");
    private static final String DEFAULT_TRIGGER_DIR = "triggers";

    private static final String OPERATING_SYSTEM = System.getProperty("os.name").toLowerCase();
    public static final boolean isLinux = OPERATING_SYSTEM.contains("linux");

    private static volatile InetAddress localInetAddress;
    private static volatile InetAddress broadcastInetAddress;
    private static volatile InetAddress broadcastNativeAddress;
    private static volatile InetAddressAndPort broadcastNativeAddressAndPort;
    private static volatile InetAddressAndPort broadcastInetAddressAndPort;
    private static volatile InetAddressAndPort localInetAddressAndPort;

    private static volatile String previousReleaseVersionString;

    private static int availableProcessors = Integer.getInteger("cassandra.available_processors", DatabaseDescriptor.getAvailableProcessors());

    public static void setAvailableProcessors(int value)
    {
        availableProcessors = value;
    }

    public static int getAvailableProcessors()
    {
        if (availableProcessors > 0)
            return availableProcessors;
        else
            return Runtime.getRuntime().availableProcessors();
    }

    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;

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
     * Please use getJustBroadcastAddress instead. You need this only when you have to listen/connect. It's also missing
     * the port you should be using. 99% of code doesn't want this.
     */
    public static InetAddress getJustLocalAddress()
    {
        if (localInetAddress == null)
        {
            if (DatabaseDescriptor.getListenAddress() == null)
            {
                try
                {
                    localInetAddress = InetAddress.getLocalHost();
                    logger.info("InetAddress.getLocalHost() was used to resolve listen_address to {}, double check this is "
                                + "correct. Please check your node's config and set the listen_address in cassandra.yaml accordingly if applicable.",
                                localInetAddress);
                }
                catch(UnknownHostException e)
                {
                    logger.info("InetAddress.getLocalHost() could not resolve the address for the hostname ({}), please "
                                + "check your node's config and set the listen_address in cassandra.yaml. Falling back to {}",
                                e,
                                InetAddress.getLoopbackAddress());
                    // CASSANDRA-15901 fallback for misconfigured nodes
                    localInetAddress = InetAddress.getLoopbackAddress();
                }
            }
            else
                localInetAddress = DatabaseDescriptor.getListenAddress();
        }
        return localInetAddress;
    }

    /**
     * The address and port to listen on for intra-cluster storage traffic (not client). Use this to get the correct
     * stuff to listen on for intra-cluster communication.
     */
    public static InetAddressAndPort getLocalAddressAndPort()
    {
        if (localInetAddressAndPort == null)
        {
            if(DatabaseDescriptor.getRawConfig() == null)
            {
                localInetAddressAndPort = InetAddressAndPort.getByAddress(getJustLocalAddress());
            }
            else
            {
                localInetAddressAndPort = InetAddressAndPort.getByAddressOverrideDefaults(getJustLocalAddress(),
                                                                                          DatabaseDescriptor.getStoragePort());
            }
        }
        return localInetAddressAndPort;
    }

    /**
     * Retrieve just the broadcast address but not the port. This is almost always the wrong thing to be using because
     * it's ambiguous since you need the address and port to identify a node. You want getBroadcastAddressAndPort
     */
    public static InetAddress getJustBroadcastAddress()
    {
        if (broadcastInetAddress == null)
            broadcastInetAddress = DatabaseDescriptor.getBroadcastAddress() == null
                                 ? getJustLocalAddress()
                                 : DatabaseDescriptor.getBroadcastAddress();
        return broadcastInetAddress;
    }

    /**
     * Get the broadcast address and port for intra-cluster storage traffic. This the address to advertise that uniquely
     * identifies the node and is reachable from everywhere. This is the one you want unless you are trying to connect
     * to the local address specifically.
     */
    public static InetAddressAndPort getBroadcastAddressAndPort()
    {
        if (broadcastInetAddressAndPort == null)
        {
            if(DatabaseDescriptor.getRawConfig() == null)
            {
                broadcastInetAddressAndPort = InetAddressAndPort.getByAddress(getJustBroadcastAddress());
            }
            else
            {
                broadcastInetAddressAndPort = InetAddressAndPort.getByAddressOverrideDefaults(getJustBroadcastAddress(),
                                                                                              DatabaseDescriptor.getStoragePort());
            }
        }
        return broadcastInetAddressAndPort;
    }

    /**
     * <b>THIS IS FOR TESTING ONLY!!</b>
     */
    public static void setBroadcastInetAddress(InetAddress addr)
    {
        broadcastInetAddress = addr;
        broadcastInetAddressAndPort = InetAddressAndPort.getByAddress(broadcastInetAddress);
    }

    /**
     * <b>THIS IS FOR TESTING ONLY!!</b>
     */
    public static void setBroadcastInetAddressAndPort(InetAddressAndPort addr)
    {
        broadcastInetAddress = addr.getAddress();
        broadcastInetAddressAndPort = addr;
    }

    /**
     * This returns the address that is bound to for the native protocol for communicating with clients. This is ambiguous
     * because it doesn't include the port and it's almost always the wrong thing to be using you want getBroadcastNativeAddressAndPort
     */
    public static InetAddress getJustBroadcastNativeAddress()
    {
        if (broadcastNativeAddress == null)
            broadcastNativeAddress = DatabaseDescriptor.getBroadcastRpcAddress() == null
                                   ? DatabaseDescriptor.getRpcAddress()
                                   : DatabaseDescriptor.getBroadcastRpcAddress();
        return broadcastNativeAddress;
    }

    /**
     * This returns the address that is bound to for the native protocol for communicating with clients. This is almost
     * always what you need to identify a node and how to connect to it as a client.
     */
    public static InetAddressAndPort getBroadcastNativeAddressAndPort()
    {
        if (broadcastNativeAddressAndPort == null)
            if(DatabaseDescriptor.getRawConfig() == null)
            {
                broadcastNativeAddressAndPort = InetAddressAndPort.getByAddress(getJustBroadcastNativeAddress());
            }
            else
            {
                broadcastNativeAddressAndPort = InetAddressAndPort.getByAddressOverrideDefaults(getJustBroadcastNativeAddress(),
                                                                                                DatabaseDescriptor.getNativeTransportPort());
            }
        return broadcastNativeAddressAndPort;
    }

    public static String getNetworkInterface(InetAddress localAddress)
    {
        try
        {
            for(NetworkInterface ifc : Collections.list(NetworkInterface.getNetworkInterfaces()))
            {
                if(ifc.isUp())
                {
                    for(InetAddress addr : Collections.list(ifc.getInetAddresses()))
                    {
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

        return new File(scpurl.getFile()).absolutePath();
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

    public static void setPreviousReleaseVersionString(String previousReleaseVersionString)
    {
        FBUtilities.previousReleaseVersionString = previousReleaseVersionString;
    }

    public static String getPreviousReleaseVersionString()
    {
        return previousReleaseVersionString;
    }

    private static final Supplier<String> loadedVersionString = Suppliers.memoize(() -> {
        try (InputStream in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties"))
        {
            if (in == null)
                return null;
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
    });

    public static String getReleaseVersionString()
    {
        String v = loadedVersionString.get();
        return v != null ? v : System.getProperty("cassandra.releaseVersion", UNKNOWN_RELEASE_VERSION);
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
        return currentTimeMillis() * 1000;
    }

    public static int nowInSeconds()
    {
        return (int) (currentTimeMillis() / 1000);
    }

    public static Instant now()
    {
        long epochMilli = currentTimeMillis();
        return Instant.ofEpochMilli(epochMilli);
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
            endNanos = nanoTime() + units.toNanos(timeout);
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
                    long waitFor = Math.max(1, endNanos - nanoTime());
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
            throw Throwables.cleaned(ee);
        }
        catch (InterruptedException ie)
        {
            throw new UncheckedInterruptedException(ie);
        }
    }

    public static <T, F extends Future<? extends T>> F waitOnFirstFuture(Iterable<? extends F> futures)
    {
        return waitOnFirstFuture(futures, 100);
    }
    /**
     * Only wait for the first future to finish from a list of futures. Will block until at least 1 future finishes.
     * @param futures The futures to wait on
     * @return future that completed.
     */
    public static <T, F extends Future<? extends T>> F waitOnFirstFuture(Iterable<? extends F> futures, long delay)
    {
        while (true)
        {
            Iterator<? extends F> iter = futures.iterator();
            if (!iter.hasNext())
                throw new IllegalArgumentException();

            while (true)
            {
                F f = iter.next();
                boolean isDone;
                if ((isDone = f.isDone()) || !iter.hasNext())
                {
                    try
                    {
                        f.get(delay, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                    catch (ExecutionException e)
                    {
                        throw new RuntimeException(e);
                    }
                    catch (TimeoutException e)
                    {
                        if (!isDone) // prevent infinite loops on bad implementations (not encountered)
                            break;
                    }
                    return f;
                }
            }
        }
    }

    /**
     * Returns a new {@link Future} wrapping the given list of futures and returning a list of their results.
     */
    public static <T> org.apache.cassandra.utils.concurrent.Future<List<T>> allOf(Collection<? extends org.apache.cassandra.utils.concurrent.Future<? extends T>> futures)
    {
        return FutureCombiner.allOf(futures);
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
        return newPartitioner(validationMetadata.partitioner, Optional.of(header.getKeyType()));
    }

    public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException
    {
        return newPartitioner(partitionerClassName, Optional.empty());
    }

    @VisibleForTesting
    static IPartitioner newPartitioner(String partitionerClassName, Optional<AbstractType<?>> comparator) throws ConfigurationException
    {
        if (!partitionerClassName.contains("."))
            partitionerClassName = "org.apache.cassandra.dht." + partitionerClassName;

        if (partitionerClassName.equals("org.apache.cassandra.dht.LocalPartitioner"))
        {
            assert comparator.isPresent() : "Expected a comparator for local partitioner";
            return new LocalPartitioner(comparator.get());
        }
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

    public static INetworkAuthorizer newNetworkAuthorizer(String className)
    {
        if (className == null)
        {
            return new AllowAllNetworkAuthorizer();
        }
        if (!className.contains("."))
        {
            className = "org.apache.cassandra.auth." + className;
        }
        return FBUtilities.construct(className, "network authorizer");
    }

    public static IAuditLogger newAuditLogger(String className, Map<String, String> parameters) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.audit." + className;

        try
        {
            Class<?> auditLoggerClass = FBUtilities.classForName(className, "Audit logger");
            return (IAuditLogger) auditLoggerClass.getConstructor(Map.class).newInstance(parameters);
        }
        catch (Exception ex)
        {
            throw new ConfigurationException("Unable to create instance of IAuditLogger.", ex);
        }
    }

    public static ISslContextFactory newSslContextFactory(String className, Map<String,Object> parameters) throws ConfigurationException
    {
        if (!className.contains("."))
            className = "org.apache.cassandra.security." + className;

        try
        {
            Class<?> sslContextFactoryClass = Class.forName(className);
            return (ISslContextFactory) sslContextFactoryClass.getConstructor(Map.class).newInstance(parameters);
        }
        catch (Exception ex)
        {
            throw new ConfigurationException("Unable to create instance of ISslContextFactory for " + className, ex);
        }
    }

    public static IAutoRepairTokenRangeSplitter newAutoRepairTokenRangeSplitter(ParameterizedClass parameterizedClass) throws ConfigurationException
    {
        String className = parameterizedClass.class_name.contains(".") ?
                           parameterizedClass.class_name :
                           "org.apache.cassandra.repair.autorepair." + parameterizedClass.class_name;

        try
        {
            Class<?> tokenRangeSplitterClass = Class.forName(className);
            try
            {
                Map<String, String> parameters = parameterizedClass.parameters != null ? parameterizedClass.parameters : Collections.emptyMap();
                // first attempt to initialize with Map arguments.
                return (IAutoRepairTokenRangeSplitter) tokenRangeSplitterClass.getConstructor(Map.class).newInstance(parameters);
            }
            catch (NoSuchMethodException nsme)
            {
                // fall back on no argument constructor.
                return (IAutoRepairTokenRangeSplitter)  tokenRangeSplitterClass.getConstructor().newInstance();
            }
        }
        catch (Exception ex)
        {
            throw new ConfigurationException("Unable to create instance of IAutoRepairTokenRangeSplitter for " + className, ex);
        }
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

    public static void serializeToJsonFile(Object object, File outputFile) throws IOException
    {
        try (FileOutputStreamPlus out = outputFile.newOutputStream(OVERWRITE))
        {
            jsonMapper.writeValue((OutputStream) out, object);
        }
    }

    public static <T> T deserializeFromJsonFile(Class<T> tClass, File file) throws IOException
    {
        try (FileInputStreamPlus in = file.newInputStream())
        {
            return jsonMapper.readValue((InputStream) in, tClass);
        }
    }

    public static String prettyPrintMemory(long size)
    {
        return prettyPrintMemory(size, false);
    }

    public static String prettyPrintMemory(long size, boolean includeSpace)
    {
        if (size >= 1 << 30)
            return String.format("%.3f%sGiB", size / (double) (1 << 30), includeSpace ? " " : "");
        if (size >= 1 << 20)
            return String.format("%.3f%sMiB", size / (double) (1 << 20), includeSpace ? " " : "");
        return String.format("%.3f%sKiB", size / (double) (1 << 10), includeSpace ? " " : "");
    }

    public static String prettyPrintMemoryPerSecond(long rate)
    {
        if (rate >= 1 << 30)
            return String.format("%.3fGiB/s", rate / (double) (1 << 30));
        if (rate >= 1 << 20)
            return String.format("%.3fMiB/s", rate / (double) (1 << 20));
        return String.format("%.3fKiB/s", rate / (double) (1 << 10));
    }

    public static String prettyPrintMemoryPerSecond(long bytes, long timeInNano)
    {
        // We can't sanely calculate a rate over 0 nanoseconds
        if (timeInNano == 0)
            return "NaN  KiB/s";

        long rate = (long) (((double) bytes / timeInNano) * 1000 * 1000 * 1000);

        return prettyPrintMemoryPerSecond(rate);
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
                    String lineSep = LINE_SEPARATOR.getString();
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
            throw new UncheckedInterruptedException(e);
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
        File historyDir = new File(USER_HOME.getString(), ".cassandra");
        FileUtils.createDirectory(historyDir);
        return historyDir;
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

	public static void sleepQuietly(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    public static long align(long val, int boundary)
    {
        return (val + boundary) & ~(boundary - 1);
    }

    @VisibleForTesting
    public static void reset()
    {
        localInetAddress = null;
        localInetAddressAndPort = null;
        broadcastInetAddress = null;
        broadcastInetAddressAndPort = null;
        broadcastNativeAddress = null;
    }

    /**
     * Hack to prevent the ugly "illegal access" warnings in Java 11+ like the following.
     */
    public static void preventIllegalAccessWarnings()
    {
        // Example "annoying" trace:
        //        WARNING: An illegal reflective access operation has occurred
        //        WARNING: Illegal reflective access by io.netty.util.internal.ReflectionUtil (file:...)
        //        WARNING: Please consider reporting this to the maintainers of io.netty.util.internal.ReflectionUtil
        //        WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
        //        WARNING: All illegal access operations will be denied in a future release
        try
        {
            Class<?> c = Class.forName("jdk.internal.module.IllegalAccessLogger");
            Field f = c.getDeclaredField("logger");
            f.setAccessible(true);
            f.set(null, null);
        }
        catch (Exception e)
        {
            // ignore
        }
    }

    public static String camelToSnake(String camel)
    {
        StringBuilder sb = new StringBuilder();
        for (char c : camel.toCharArray())
        {
            if (Character.isUpperCase(c))
            {
                // if first char is uppercase, then avoid adding the _ prefix
                if (sb.length() > 0)
                    sb.append('_');
                sb.append(Character.toLowerCase(c));
            }
            else
            {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
