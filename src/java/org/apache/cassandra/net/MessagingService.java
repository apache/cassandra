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
package org.apache.cassandra.net;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.AntiEntropyService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.streaming.compress.CompressedFileStreamTask;
import org.apache.cassandra.utils.*;
import org.cliffc.high_scale_lib.NonBlockingHashMap;


public final class MessagingService implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    // 8 bits version, so don't waste versions
    // We are no longer compatible with versions older than 1.0
    public static final int VERSION_10 = 3;
    public static final int VERSION_11 = 4;
    public static final int VERSION_12 = 5;
    public static final int current_version = VERSION_12;

    /** we preface every message with this number so the recipient can validate the sender is sane */
    static final int PROTOCOL_MAGIC = 0xCA552DFA;

    /* All verb handler identifiers */
    public enum Verb
    {
        MUTATION,
        @Deprecated BINARY,
        READ_REPAIR,
        READ,
        REQUEST_RESPONSE, // client-initiated reads and writes
        @Deprecated STREAM_INITIATE,
        @Deprecated STREAM_INITIATE_DONE,
        STREAM_REPLY,
        STREAM_REQUEST,
        RANGE_SLICE,
        BOOTSTRAP_TOKEN,
        TREE_REQUEST,
        TREE_RESPONSE,
        @Deprecated JOIN,
        GOSSIP_DIGEST_SYN,
        GOSSIP_DIGEST_ACK,
        GOSSIP_DIGEST_ACK2,
        @Deprecated DEFINITIONS_ANNOUNCE,
        DEFINITIONS_UPDATE,
        TRUNCATE,
        SCHEMA_CHECK,
        @Deprecated INDEX_SCAN,
        REPLICATION_FINISHED,
        INTERNAL_RESPONSE, // responses to internal calls
        COUNTER_MUTATION,
        STREAMING_REPAIR_REQUEST,
        STREAMING_REPAIR_RESPONSE,
        SNAPSHOT, // Similar to nt snapshot
        MIGRATION_REQUEST,
        GOSSIP_SHUTDOWN,
        // use as padding for backwards compatability where a previous version needs to validate a verb from the future.
        UNUSED_1,
        UNUSED_2,
        UNUSED_3,
        ;
        // remember to add new verbs at the end, since we serialize by ordinal
    }
    public static final Verb[] VERBS = Verb.values();

    public static final EnumMap<MessagingService.Verb, Stage> verbStages = new EnumMap<MessagingService.Verb, Stage>(MessagingService.Verb.class)
    {{
        put(Verb.MUTATION, Stage.MUTATION);
        put(Verb.BINARY, Stage.MUTATION);
        put(Verb.READ_REPAIR, Stage.MUTATION);
        put(Verb.TRUNCATE, Stage.MUTATION);
        put(Verb.READ, Stage.READ);
        put(Verb.REQUEST_RESPONSE, Stage.REQUEST_RESPONSE);
        put(Verb.STREAM_REPLY, Stage.MISC); // TODO does this really belong on misc? I've just copied old behavior here
        put(Verb.STREAM_REQUEST, Stage.STREAM);
        put(Verb.RANGE_SLICE, Stage.READ);
        put(Verb.BOOTSTRAP_TOKEN, Stage.MISC);
        put(Verb.TREE_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.TREE_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_REQUEST, Stage.ANTI_ENTROPY);
        put(Verb.STREAMING_REPAIR_RESPONSE, Stage.ANTI_ENTROPY);
        put(Verb.GOSSIP_DIGEST_ACK, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_ACK2, Stage.GOSSIP);
        put(Verb.GOSSIP_DIGEST_SYN, Stage.GOSSIP);
        put(Verb.GOSSIP_SHUTDOWN, Stage.GOSSIP);
        put(Verb.DEFINITIONS_UPDATE, Stage.MIGRATION);
        put(Verb.SCHEMA_CHECK, Stage.MIGRATION);
        put(Verb.MIGRATION_REQUEST, Stage.MIGRATION);
        put(Verb.INDEX_SCAN, Stage.READ);
        put(Verb.REPLICATION_FINISHED, Stage.MISC);
        put(Verb.INTERNAL_RESPONSE, Stage.INTERNAL_RESPONSE);
        put(Verb.COUNTER_MUTATION, Stage.MUTATION);
        put(Verb.SNAPSHOT, Stage.MISC);
        put(Verb.UNUSED_1, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_2, Stage.INTERNAL_RESPONSE);
        put(Verb.UNUSED_3, Stage.INTERNAL_RESPONSE);
    }};

    /**
     * Messages we receive in IncomingTcpConnection have a Verb that tells us what kind of message it is.
     * Most of the time, this is enough to determine how to deserialize the message payload.
     * The exception is the REQUEST_RESPONSE verb, which just means "a reply to something you told me to do."
     * Traditionally, this was fine since each VerbHandler knew what type of payload it expected, and
     * handled the deserialization itself.  Now that we do that in ITC, to avoid the extra copy to an
     * intermediary byte[] (See CASSANDRA-3716), we need to wire that up to the CallbackInfo object
     * (see below).
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> verbSerializers = new EnumMap<Verb, IVersionedSerializer<?>>(Verb.class)
    {{
        put(Verb.REQUEST_RESPONSE, CallbackDeterminedSerializer.instance);
        put(Verb.INTERNAL_RESPONSE, CallbackDeterminedSerializer.instance);

        put(Verb.MUTATION, RowMutation.serializer);
        put(Verb.READ_REPAIR, RowMutation.serializer);
        put(Verb.READ, ReadCommand.serializer);
        put(Verb.STREAM_REPLY, StreamReply.serializer);
        put(Verb.STREAM_REQUEST, StreamRequest.serializer);
        put(Verb.RANGE_SLICE, RangeSliceCommand.serializer);
        put(Verb.BOOTSTRAP_TOKEN, BootStrapper.StringSerializer.instance);
        put(Verb.TREE_REQUEST, AntiEntropyService.TreeRequest.serializer);
        put(Verb.TREE_RESPONSE, AntiEntropyService.Validator.serializer);
        put(Verb.STREAMING_REPAIR_REQUEST, StreamingRepairTask.serializer);
        put(Verb.STREAMING_REPAIR_RESPONSE, UUIDGen.serializer);
        put(Verb.GOSSIP_DIGEST_ACK, GossipDigestAck.serializer);
        put(Verb.GOSSIP_DIGEST_ACK2, GossipDigestAck2.serializer);
        put(Verb.GOSSIP_DIGEST_SYN, GossipDigestSyn.serializer);
        put(Verb.DEFINITIONS_UPDATE, MigrationManager.MigrationsSerializer.instance);
        put(Verb.TRUNCATE, Truncation.serializer);
        put(Verb.INDEX_SCAN, IndexScanCommand.serializer);
        put(Verb.REPLICATION_FINISHED, null);
        put(Verb.COUNTER_MUTATION, CounterMutation.serializer);
    }};

    /**
     * A Map of what kind of serializer to wire up to a REQUEST_RESPONSE callback, based on outbound Verb.
     */
    public static final EnumMap<Verb, IVersionedSerializer<?>> callbackDeserializers = new EnumMap<Verb, IVersionedSerializer<?>>(Verb.class)
    {{
        put(Verb.MUTATION, WriteResponse.serializer);
        put(Verb.READ_REPAIR, WriteResponse.serializer);
        put(Verb.COUNTER_MUTATION, WriteResponse.serializer);
        put(Verb.RANGE_SLICE, RangeSliceReply.serializer);
        put(Verb.READ, ReadResponse.serializer);
        put(Verb.TRUNCATE, TruncateResponse.serializer);
        put(Verb.SNAPSHOT, null);

        put(Verb.MIGRATION_REQUEST, MigrationManager.MigrationsSerializer.instance);
        put(Verb.SCHEMA_CHECK, UUIDGen.serializer);
        put(Verb.BOOTSTRAP_TOKEN, BootStrapper.StringSerializer.instance);
        put(Verb.REPLICATION_FINISHED, null);
    }};

    /* This records all the results mapped by message Id */
    private final ExpiringMap<String, CallbackInfo> callbacks;

    /**
     * a placeholder class that means "deserialize using the callback." We can't implement this without
     * special-case code in InboundTcpConnection because there is no way to pass the message id to IVersionedSerializer.
     */
    static class CallbackDeterminedSerializer implements IVersionedSerializer<Object>
    {
        public static final CallbackDeterminedSerializer instance = new CallbackDeterminedSerializer();

        public Object deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public void serialize(Object o, DataOutput out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(Object o, int version)
        {
            throw new UnsupportedOperationException();
        }
    }

    /* Lookup table for registering message handlers based on the verb. */
    private final Map<Verb, IVerbHandler> verbHandlers;

    /** One executor per destination InetAddress for streaming.
     *
     * See CASSANDRA-3494 for the background. We have streaming in place so we do not want to limit ourselves to
     * one stream at a time for throttling reasons. But, we also do not want to just arbitrarily stream an unlimited
     * amount of files at once because a single destination might have hundreds of files pending and it would cause a
     * seek storm. So, transfer exactly one file per destination host. That puts a very natural rate limit on it, in
     * addition to mapping well to the expected behavior in many cases.
     *
     * We will create our stream executors with a core size of 0 so that they time out and do not consume threads. This
     * means the overhead in the degenerate case of having streamed to everyone in the ring over time as a ring changes,
     * is not going to be a thread per node - but rather an instance per node. That's totally fine.
     */
    private final ConcurrentMap<InetAddress, DebuggableThreadPoolExecutor> streamExecutors = new NonBlockingHashMap<InetAddress, DebuggableThreadPoolExecutor>();
    private final AtomicInteger activeStreamsOutbound = new AtomicInteger(0);

    private final NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool> connectionManagers = new NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool>();

    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    private final List<SocketThread> socketThreads = Lists.newArrayList();
    private final SimpleCondition listenGate;

    /**
     * Verbs it's okay to drop if the request has been queued longer than RPC_TIMEOUT.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    public static final EnumSet<Verb> DROPPABLE_VERBS = EnumSet.of(Verb.BINARY,
                                                                                  Verb.MUTATION,
                                                                                  Verb.READ_REPAIR,
                                                                                  Verb.READ,
                                                                                  Verb.RANGE_SLICE,
                                                                                  Verb.REQUEST_RESPONSE);

    // total dropped message counts for server lifetime
    private final Map<Verb, AtomicInteger> droppedMessages = new EnumMap<Verb, AtomicInteger>(Verb.class);
    // dropped count when last requested for the Recent api.  high concurrency isn't necessary here.
    private final Map<Verb, Integer> lastDropped = Collections.synchronizedMap(new EnumMap<Verb, Integer>(Verb.class));
    private final Map<Verb, Integer> lastDroppedInternal = new EnumMap<Verb, Integer>(Verb.class);

    private long totalTimeouts = 0;
    private long recentTotalTimeouts = 0;
    private final Map<String, AtomicLong> timeoutsPerHost = new HashMap<String, AtomicLong>();
    private final Map<String, AtomicLong> recentTimeoutsPerHost = new HashMap<String, AtomicLong>();
    private final List<ILatencySubscriber> subscribers = new ArrayList<ILatencySubscriber>();

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddress, Integer> versions = new NonBlockingHashMap<InetAddress, Integer>();

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService();
    }
    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    private MessagingService()
    {
        for (Verb verb : DROPPABLE_VERBS)
        {
            droppedMessages.put(verb, new AtomicInteger());
            lastDropped.put(verb, 0);
            lastDroppedInternal.put(verb, 0);
        }

        listenGate = new SimpleCondition();
        verbHandlers = new EnumMap<Verb, IVerbHandler>(Verb.class);
        Runnable logDropped = new Runnable()
        {
            public void run()
            {
                logDroppedMessages();
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(logDropped, LOG_DROPPED_INTERVAL_IN_MS, LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);

        Function<Pair<String, ExpiringMap.CacheableObject<CallbackInfo>>, ?> timeoutReporter = new Function<Pair<String, ExpiringMap.CacheableObject<CallbackInfo>>, Object>()
        {
            public Object apply(Pair<String, ExpiringMap.CacheableObject<CallbackInfo>> pair)
            {
                CallbackInfo expiredCallbackInfo = pair.right.value;
                maybeAddLatency(expiredCallbackInfo.callback, expiredCallbackInfo.target, (double) pair.right.timeout);
                totalTimeouts++;
                String ip = expiredCallbackInfo.target.getHostAddress();
                AtomicLong c = timeoutsPerHost.get(ip);
                if (c == null)
                {
                    c = new AtomicLong();
                    timeoutsPerHost.put(ip, c);
                }
                c.incrementAndGet();
                // we only create AtomicLong instances here, so that the write
                // access to the hashmap happens single-threadedly.
                if (recentTimeoutsPerHost.get(ip) == null)
                    recentTimeoutsPerHost.put(ip, new AtomicLong());

                if (expiredCallbackInfo.shouldHint())
                {
                    assert expiredCallbackInfo.sentMessage != null;
                    RowMutation rm = (RowMutation) expiredCallbackInfo.sentMessage.payload;
                    return StorageProxy.scheduleLocalHint(rm, expiredCallbackInfo.target, null, null);
                }

                return null;
            }
        };

        callbacks = new ExpiringMap<String, CallbackInfo>(DatabaseDescriptor.getMinRpcTimeout(), timeoutReporter);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Track latency information for the dynamic snitch
     * @param cb the callback associated with this message -- this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     * @param latency
     */
    public void maybeAddLatency(IMessageCallback cb, InetAddress address, double latency)
    {
        if (cb.isLatencyForSnitch())
            addLatency(address, latency);
    }

    public void addLatency(InetAddress address, double latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(address, latency);
    }

    /** called from gossiper when it notices a node is not responding. */
    public void convict(InetAddress ep)
    {
        logger.debug("Resetting pool for " + ep);
        getConnectionPool(ep).reset();
    }

    /**
     * Listen on the specified port.
     * @param localEp InetAddress whose port to listen on.
     */
    public void listen(InetAddress localEp) throws IOException, ConfigurationException
    {
        callbacks.reset(); // hack to allow tests to stop/restart MS
        for (ServerSocket ss: getServerSocket(localEp))
        {
            SocketThread th = new SocketThread(ss, "ACCEPT-" + localEp);
            th.start();
            socketThreads.add(th);
        }
        listenGate.signalAll();
    }

    private List<ServerSocket> getServerSocket(InetAddress localEp) throws IOException, ConfigurationException
    {
       final List<ServerSocket> ss = new ArrayList<ServerSocket>(2);
        if (DatabaseDescriptor.getEncryptionOptions().internode_encryption != EncryptionOptions.InternodeEncryption.none)
        {
            ss.add(SSLFactory.getServerSocket(DatabaseDescriptor.getEncryptionOptions(), localEp, DatabaseDescriptor.getSSLStoragePort()));
            // setReuseAddress happens in the factory.
            logger.info("Starting Encrypted Messaging Service on SSL port {}", DatabaseDescriptor.getSSLStoragePort());
        }

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        ServerSocket socket = serverChannel.socket();
        socket.setReuseAddress(true);
        InetSocketAddress address = new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort());
        try
        {
            socket.bind(address);
        }
        catch (BindException e)
        {
            if (e.getMessage().contains("in use"))
                throw new ConfigurationException(address + " is in use by another process.  Change listen_address:storage_port in cassandra.yaml to values that do not conflict with other services");
            else if (e.getMessage().contains("Cannot assign requested address"))
                throw new ConfigurationException("Unable to bind to address " + address
                        + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            else
                throw e;
        }
        logger.info("Starting Messaging Service on port {}", DatabaseDescriptor.getStoragePort());
        ss.add(socket);
        return ss;
    }

    public void waitUntilListening()
    {
        try
        {
            listenGate.await();
        }
        catch (InterruptedException ie)
        {
            logger.debug("await interrupted");
        }
    }

    public OutboundTcpConnectionPool getConnectionPool(InetAddress to)
    {
        OutboundTcpConnectionPool cp = connectionManagers.get(to);
        if (cp == null)
        {
            connectionManagers.putIfAbsent(to, new OutboundTcpConnectionPool(to));
            cp = connectionManagers.get(to);
        }
        return cp;
    }

    public OutboundTcpConnection getConnection(InetAddress to, MessageOut msg)
    {
        return getConnectionPool(to).getConnection(msg);
    }

    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     * @param verb
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(Verb verb, IVerbHandler verbHandler)
    {
        assert !verbHandlers.containsKey(verb);
        verbHandlers.put(verb, verbHandler);
    }

    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     * @param type for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(Verb type)
    {
        return verbHandlers.get(type);
    }

    public String addCallback(IMessageCallback cb, MessageOut message, InetAddress to, long timeout)
    {
        String messageId = nextId();
        CallbackInfo previous;

        // If HH is enabled and this is a mutation message => store the message to track for potential hints.
        if (DatabaseDescriptor.hintedHandoffEnabled() && message.verb == Verb.MUTATION)
            previous = callbacks.put(messageId, new CallbackInfo(to, cb, message, callbackDeserializers.get(message.verb)), timeout);
        else
            previous = callbacks.put(messageId, new CallbackInfo(to, cb, callbackDeserializers.get(message.verb)), timeout);

        assert previous == null;
        return messageId;
    }

    private static final AtomicInteger idGen = new AtomicInteger(0);
    // TODO make these integers to avoid unnecessary int -> string -> int conversions
    private static String nextId()
    {
        return Integer.toString(idGen.incrementAndGet());
    }

    /*
     * @see #sendRR(Message message, InetAddress to, IMessageCallback cb, long timeout)
     */
    public String sendRR(MessageOut message, InetAddress to, IMessageCallback cb)
    {
        return sendRR(message, to, cb, message.getTimeout());
    }

    /**
     * Send a message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * Also holds the message (only mutation messages) to determine if it
     * needs to trigger a hint (uses StorageProxy for that).
     * @param message message to be sent.
     * @param to endpoint to which the message needs to be sent
     * @param cb callback interface which is used to pass the responses or
     *           suggest that a timeout occurred to the invoker of the send().
     *           suggest that a timeout occurred to the invoker of the send().
     * @param timeout the timeout used for expiration
     * @return an reference to message id used to match with the result
     */
    public String sendRR(MessageOut message, InetAddress to, IMessageCallback cb, long timeout)
    {
        String id = addCallback(cb, message, to, timeout);
        sendOneWay(message, id, to);
        return id;
    }

    public void sendOneWay(MessageOut message, InetAddress to)
    {
        sendOneWay(message, nextId(), to);
    }

    public void sendReply(MessageOut message, String id, InetAddress to)
    {
        sendOneWay(message, id, to);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     * @param message messages to be sent.
     * @param to endpoint to which the message needs to be sent
     */
    public void sendOneWay(MessageOut message, String id, InetAddress to)
    {
        if (logger.isTraceEnabled())
            logger.trace(FBUtilities.getBroadcastAddress() + " sending " + message.verb + " to " + id + "@" + to);

        if (to.equals(FBUtilities.getBroadcastAddress()))
            logger.debug("Message-to-self {} going over MessagingService", message);

        // message sinks are a testing hook
        MessageOut processedMessage = SinkManager.processOutboundMessage(message, id, to);
        if (processedMessage == null)
        {
            return;
        }

        // get pooled connection (really, connection queue)
        OutboundTcpConnection connection = getConnection(to, processedMessage);

        // write it
        connection.enqueue(processedMessage, id);
    }

    public <T> IAsyncResult<T> sendRR(MessageOut message, InetAddress to)
    {
        IAsyncResult<T> iar = new AsyncResult();
        sendRR(message, to, iar);
        return iar;
    }

    /**
     * Stream a file from source to destination. This is highly optimized
     * to not hold any of the contents of the file in memory.
     * @param header Header contains file to stream and other metadata.
     * @param to endpoint to which we need to stream the file.
    */

    public void stream(StreamHeader header, InetAddress to)
    {
        DebuggableThreadPoolExecutor executor = streamExecutors.get(to);
        if (executor == null)
        {
            // Using a core pool size of 0 is important. See documentation of streamExecutors.
            executor = DebuggableThreadPoolExecutor.createWithMaximumPoolSize("Streaming to " + to, 1, 1, TimeUnit.SECONDS);
            DebuggableThreadPoolExecutor old = streamExecutors.putIfAbsent(to, executor);
            if (old != null)
            {
                executor.shutdown();
                executor = old;
            }
        }

        executor.execute(header.file == null || header.file.compressionInfo == null
                                 ? new FileStreamTask(header, to)
                                 : new CompressedFileStreamTask(header, to));
    }

    public void incrementActiveStreamsOutbound()
    {
        activeStreamsOutbound.incrementAndGet();
    }

    public void decrementActiveStreamsOutbound()
    {
        activeStreamsOutbound.decrementAndGet();
    }

    /** The count of active outbound stream tasks. */
    public int getActiveStreamsOutbound()
    {
        return activeStreamsOutbound.get();
    }

    public void register(ILatencySubscriber subcriber)
    {
        subscribers.add(subcriber);
    }

    public void clearCallbacksUnsafe()
    {
        callbacks.reset();
    }

    public void waitForStreaming() throws InterruptedException
    {
        // this does not prevent new streams from beginning after a drain begins, but since streams are only
        // started in response to explicit operator action (bootstrap/move/repair/etc) that feels like a feature.
        for (DebuggableThreadPoolExecutor e : streamExecutors.values())
            e.shutdown();

        for (DebuggableThreadPoolExecutor e : streamExecutors.values())
        {
            if (e.awaitTermination(24, TimeUnit.HOURS))
                logger.error("Stream took more than 24H to complete; skipping");
        }
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        logger.info("Waiting for messaging service to quiesce");
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !StageManager.getStage(Stage.MUTATION).isShutdown();

        // the important part
        callbacks.shutdownBlocking();

        // attempt to humor tests that try to stop and restart MS
        try
        {
            for (SocketThread th : socketThreads)
                th.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public void receive(MessageIn message, String id)
    {
        if (logger.isTraceEnabled())
            logger.trace(FBUtilities.getBroadcastAddress() + " received " + message.verb
                          + " from " + id + "@" + message.from);

        message = SinkManager.processInboundMessage(message, id);
        if (message == null)
            return;

        Runnable runnable = new MessageDeliveryTask(message, id);
        ExecutorService stage = StageManager.getStage(message.getMessageType());
        assert stage != null : "No stage for message type " + message.verb;
        stage.execute(runnable);
    }

    public void setCallbackForTests(String messageId, CallbackInfo callback)
    {
        callbacks.put(messageId, callback);
    }

    public CallbackInfo getRegisteredCallback(String messageId)
    {
        return callbacks.get(messageId);
    }

    public CallbackInfo removeRegisteredCallback(String messageId)
    {
        return callbacks.remove(messageId);
    }

    public long getRegisteredCallbackAge(String messageId)
    {
        return callbacks.getAge(messageId);
    }

    public static void validateMagic(int magic) throws IOException
    {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }

    public static int getBits(int packed, int start, int count)
    {
        return packed >>> (start + 1) - count & ~(-1 << count);
    }

    public ByteBuffer constructStreamHeader(StreamHeader streamHeader, boolean compress, int version)
    {
        int header = 0;
        // set compression bit.
        if (compress)
            header |= 4;
        // set streaming bit
        header |= 8;
        // Setting up the version bit
        header |= (version << 8);

        /* Adding the StreamHeader which contains the session Id along
         * with the pendingfile info for the stream.
         * | Session Id | Pending File Size | Pending File | Bool more files |
         * | No. of Pending files | Pending Files ... |
         */
        byte[] bytes;
        try
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            StreamHeader.serializer.serialize(streamHeader, buffer, version);
            bytes = buffer.getData();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        assert bytes.length > 0;

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + bytes.length);
        buffer.putInt(PROTOCOL_MAGIC);
        buffer.putInt(header);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    /**
     * @return the last version associated with address, or @param version if this is the first such version
     */
    public int setVersion(InetAddress address, int version)
    {
        logger.debug("Setting version {} for {}", version, address);
        Integer v = versions.put(address, version);
        return v == null ? version : v;
    }

    public void resetVersion(InetAddress endpoint)
    {
        logger.debug("Reseting version for {}", endpoint);
        versions.remove(endpoint);
    }

    public Integer getVersion(InetAddress address)
    {
        Integer v = versions.get(address);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            logger.trace("Assuming current protocol version for {}", address);
            return MessagingService.current_version;
        }
        else
            return v;
    }

    public int getVersion(String address) throws UnknownHostException
    {
        return getVersion(InetAddress.getByName(address));
    }

    public boolean knowsVersion(InetAddress endpoint)
    {
        return versions.get(endpoint) != null;
    }

    public void incrementDroppedMessages(Verb verb)
    {
        assert DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
        droppedMessages.get(verb).incrementAndGet();
    }

    private void logDroppedMessages()
    {
        boolean logTpstats = false;
        for (Map.Entry<Verb, AtomicInteger> entry : droppedMessages.entrySet())
        {
            AtomicInteger dropped = entry.getValue();
            Verb verb = entry.getKey();
            int recent = dropped.get() - lastDroppedInternal.get(verb);
            if (recent > 0)
            {
                logTpstats = true;
                logger.info("{} {} messages dropped in last {}ms",
                             new Object[] {recent, verb, LOG_DROPPED_INTERVAL_IN_MS});
                lastDroppedInternal.put(verb, dropped.get());
            }
        }

        if (logTpstats)
            StatusLogger.log();
    }

    private static class SocketThread extends Thread
    {
        private final ServerSocket server;

        SocketThread(ServerSocket server, String name)
        {
            super(name);
            this.server = server;
        }

        public void run()
        {
            while (true)
            {
                try
                {
                    Socket socket = server.accept();
                    new IncomingTcpConnection(socket).start();
                }
                catch (AsynchronousCloseException e)
                {
                    // this happens when another thread calls close().
                    logger.info("MessagingService shutting down server thread.");
                    break;
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        void close() throws IOException
        {
            server.close();
        }
    }

    public Map<String, Integer> getCommandPendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getPendingMessages());
        return pendingTasks;
    }

    public int getCommandPendingTasks(InetAddress address)
    {
        OutboundTcpConnectionPool connection = connectionManagers.get(address);
        return connection == null ? 0 : connection.cmdCon.getPendingMessages();
    }
    
    public Map<String, Long> getCommandCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Long> getCommandDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            droppedTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getDroppedMessages());
        return droppedTasks;
    }

    public Map<String, Integer> getResponsePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().ackCon.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getResponseCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().ackCon.getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<Verb, AtomicInteger> entry : droppedMessages.entrySet())
            map.put(entry.getKey().toString(), entry.getValue().get());
        return map;
    }

    public Map<String, Integer> getRecentlyDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<Verb, AtomicInteger> entry : droppedMessages.entrySet())
        {
            Verb verb = entry.getKey();
            Integer dropped = entry.getValue().get();
            Integer recentlyDropped = dropped - lastDropped.get(verb);
            map.put(verb.toString(), recentlyDropped);
            lastDropped.put(verb, dropped);
        }
        return map;
    }

    public long getTotalTimeouts()
    {
        return totalTimeouts;
    }

    public long getRecentTotalTimouts()
    {
        long recent = totalTimeouts - recentTotalTimeouts;
        recentTotalTimeouts = totalTimeouts;
        return recent;
    }

    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry: timeoutsPerHost.entrySet())
        {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    public Map<String, Long> getRecentTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry: recentTimeoutsPerHost.entrySet())
        {
            String ip = entry.getKey();
            AtomicLong recent = entry.getValue();
            Long timeout = timeoutsPerHost.get(ip).get();
            result.put(ip, timeout - recent.getAndSet(timeout));
        }
        return result;
    }
}
