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

package org.apache.cassandra.net;

import java.io.IOError;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
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
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.net.io.SerializerType;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.utils.*;
import org.cliffc.high_scale_lib.NonBlockingHashMap;


public final class MessagingService implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    // 8 bits version, so don't waste versions
    public static final int VERSION_07 = 1;
    public static final int VERSION_080 = 2;
    public static final int VERSION_10 = 3;
    public static final int version_ = VERSION_10;

    static SerializerType serializerType_ = SerializerType.BINARY;

    /** we preface every message with this number so the recipient can validate the sender is sane */
    static final int PROTOCOL_MAGIC = 0xCA552DFA;

    /* This records all the results mapped by message Id */
    private final ExpiringMap<String, CallbackInfo> callbacks;

    /* Lookup table for registering message handlers based on the verb. */
    private final Map<StorageService.Verb, IVerbHandler> verbHandlers_;

    /* Thread pool to handle messaging write activities */
    private final DebuggableThreadPoolExecutor streamExecutor_;

    private final NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool> connectionManagers_ = new NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool>();

    private static final Logger logger_ = LoggerFactory.getLogger(MessagingService.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    private List<SocketThread> socketThreads = Lists.newArrayList();
    private final SimpleCondition listenGate;

    /**
     * Verbs it's okay to drop if the request has been queued longer than RPC_TIMEOUT.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    public static final EnumSet<StorageService.Verb> DROPPABLE_VERBS = EnumSet.of(StorageService.Verb.BINARY,
                                                                                  StorageService.Verb.MUTATION,
                                                                                  StorageService.Verb.READ_REPAIR,
                                                                                  StorageService.Verb.READ,
                                                                                  StorageService.Verb.RANGE_SLICE,
                                                                                  StorageService.Verb.REQUEST_RESPONSE);

    // total dropped message counts for server lifetime
    private final Map<StorageService.Verb, AtomicInteger> droppedMessages = new EnumMap<StorageService.Verb, AtomicInteger>(StorageService.Verb.class);
    // dropped count when last requested for the Recent api.  high concurrency isn't necessary here.
    private final Map<StorageService.Verb, Integer> lastDropped = Collections.synchronizedMap(new EnumMap<StorageService.Verb, Integer>(StorageService.Verb.class));
    private final Map<StorageService.Verb, Integer> lastDroppedInternal = new EnumMap<StorageService.Verb, Integer>(StorageService.Verb.class);

    private long totalTimeouts = 0;
    private long recentTotalTimeouts = 0;
    private final Map<String, AtomicLong> timeoutsPerHost = new HashMap<String, AtomicLong>();
    private final Map<String, AtomicLong> recentTimeoutsPerHost = new HashMap<String, AtomicLong>();
    private final List<ILatencySubscriber> subscribers = new ArrayList<ILatencySubscriber>();
    private static final long DEFAULT_CALLBACK_TIMEOUT = DatabaseDescriptor.getRpcTimeout();

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
        for (StorageService.Verb verb : DROPPABLE_VERBS)
        {
            droppedMessages.put(verb, new AtomicInteger());
            lastDropped.put(verb, 0);
            lastDroppedInternal.put(verb, 0);
        }

        listenGate = new SimpleCondition();
        verbHandlers_ = new EnumMap<StorageService.Verb, IVerbHandler>(StorageService.Verb.class);
        streamExecutor_ = new DebuggableThreadPoolExecutor("Streaming", Thread.MIN_PRIORITY);
        Runnable logDropped = new Runnable()
        {
            public void run()
            {
                logDroppedMessages();
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(logDropped, LOG_DROPPED_INTERVAL_IN_MS, LOG_DROPPED_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);

        Function<Pair<String, CallbackInfo>, ?> timeoutReporter = new Function<Pair<String, CallbackInfo>, Object>()
        {
            public Object apply(Pair<String, CallbackInfo> pair)
            {
                CallbackInfo expiredCallbackInfo = pair.right;
                maybeAddLatency(expiredCallbackInfo.callback, expiredCallbackInfo.target, (double) DatabaseDescriptor.getRpcTimeout());
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
                    assert expiredCallbackInfo.message != null;
                    try
                    {
                        RowMutation rm = RowMutation.fromBytes(expiredCallbackInfo.message.getMessageBody(), expiredCallbackInfo.message.getVersion());
                        return StorageProxy.scheduleLocalHint(rm, expiredCallbackInfo.target, null, null);
                    }
                    catch (IOException e)
                    {
                        logger_.error("Unable to deserialize mutation when writting hint for: " + expiredCallbackInfo.target);
                    }
                }

                return null;
            }
        };

        callbacks = new ExpiringMap<String, CallbackInfo>(DEFAULT_CALLBACK_TIMEOUT, timeoutReporter);

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
        logger_.debug("Resetting pool for " + ep);
        getConnectionPool(ep).reset();
    }

    /**
     * Listen on the specified port.
     * @param localEp InetAddress whose port to listen on.
     */
    public void listen(InetAddress localEp) throws IOException, ConfigurationException
    {
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
       final List<ServerSocket> ss = new ArrayList<ServerSocket>();
        if (DatabaseDescriptor.getEncryptionOptions().internode_encryption != EncryptionOptions.InternodeEncryption.none)
        {
            ss.add(SSLFactory.getServerSocket(DatabaseDescriptor.getEncryptionOptions(), localEp, DatabaseDescriptor.getSSLStoragePort()));
            // setReuseAddress happens in the factory.
            logger_.info("Starting Encrypted Messaging Service on SSL port {}", DatabaseDescriptor.getSSLStoragePort());
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
        logger_.info("Starting Messaging Service on port {}", DatabaseDescriptor.getStoragePort());
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
            logger_.debug("await interrupted");
        }
    }

    public OutboundTcpConnectionPool getConnectionPool(InetAddress to)
    {
        OutboundTcpConnectionPool cp = connectionManagers_.get(to);
        if (cp == null)
        {
            connectionManagers_.putIfAbsent(to, new OutboundTcpConnectionPool(to));
            cp = connectionManagers_.get(to);
        }
        return cp;
    }

    public OutboundTcpConnection getConnection(InetAddress to, Message msg)
    {
        return getConnectionPool(to).getConnection(msg);
    }

    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     * @param verb
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(StorageService.Verb verb, IVerbHandler verbHandler)
    {
    	assert !verbHandlers_.containsKey(verb);
    	verbHandlers_.put(verb, verbHandler);
    }

    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     * @param type for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(StorageService.Verb type)
    {
        return verbHandlers_.get(type);
    }

    private void addCallback(IMessageCallback cb, String messageId, Message message, InetAddress to, long timeout)
    {
        CallbackInfo previous;

        // If HH is enabled and this is a mutation message => store the message to track for potential hints.
        if (DatabaseDescriptor.hintedHandoffEnabled() && message.getVerb() == StorageService.Verb.MUTATION)
            previous = callbacks.put(messageId, new CallbackInfo(to, cb, message), timeout);
        else
            previous = callbacks.put(messageId, new CallbackInfo(to, cb), timeout);

        assert previous == null;
    }

    private static AtomicInteger idGen = new AtomicInteger(0);
    // TODO make these integers to avoid unnecessary int -> string -> int conversions
    private static String nextId()
    {
        return Integer.toString(idGen.incrementAndGet());
    }

    /*
     * @see #sendRR(Message message, InetAddress to, IMessageCallback cb, long timeout)
     */
    public String sendRR(Message message, InetAddress to, IMessageCallback cb)
    {
        return sendRR(message, to, cb, DEFAULT_CALLBACK_TIMEOUT);
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
    public String sendRR(Message message, InetAddress to, IMessageCallback cb, long timeout)
    {
        String id = nextId();
        addCallback(cb, id, message, to, timeout);
        sendOneWay(message, id, to);
        return id;
    }

    public void sendOneWay(Message message, InetAddress to)
    {
        sendOneWay(message, nextId(), to);
    }

    public void sendReply(Message message, String id, InetAddress to)
    {
        sendOneWay(message, id, to);
    }

    /**
     * Send a message to a given endpoint. similar to sendRR(Message, InetAddress, IAsyncCallback)
     * @param producer pro
     * @param to endpoing to which the message needs to be sent
     * @param cb callback that processes responses.
     * @return a reference to the message id use to match with the result.
     */
    public String sendRR(MessageProducer producer, InetAddress to, IAsyncCallback cb)
    {
        try
        {
            return sendRR(producer.getMessage(Gossiper.instance.getVersion(to)), to, cb);
        }
        catch (IOException ex)
        {
            // happened during message creation.
            throw new IOError(ex);
        }
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     * @param message messages to be sent.
     * @param to endpoint to which the message needs to be sent
     */
    private void sendOneWay(Message message, String id, InetAddress to)
    {
        if (logger_.isTraceEnabled())
            logger_.trace(FBUtilities.getBroadcastAddress() + " sending " + message.getVerb() + " to " + id + "@" + to);

        // do local deliveries
        if ( message.getFrom().equals(to) )
        {
            receive(message, id);
            return;
        }

        // message sinks are a testing hook
        Message processedMessage = SinkManager.processClientMessage(message, id, to);
        if (processedMessage == null)
        {
            return;
        }

        // get pooled connection (really, connection queue)
        OutboundTcpConnection connection = getConnection(to, processedMessage);

        // write it
        connection.enqueue(processedMessage, id);
    }

    public IAsyncResult sendRR(Message message, InetAddress to)
    {
        IAsyncResult iar = new AsyncResult();
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
        /* Streaming asynchronously on streamExector_ threads. */
        streamExecutor_.execute(new FileStreamTask(header, to));
    }

    /** The count of active outbound stream tasks. */
    public int getActiveStreamsOutbound()
    {
        return streamExecutor_.getActiveCount();
    }

    public void register(ILatencySubscriber subcriber)
    {
        subscribers.add(subcriber);
    }

    public void waitForStreaming() throws InterruptedException
    {
        streamExecutor_.awaitTermination(24, TimeUnit.HOURS);
    }

    public void clearCallbacksUnsafe()
    {
        callbacks.clear();
    }

    public void shutdown()
    {
        logger_.info("Shutting down MessageService...");
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !StageManager.getStage(Stage.MUTATION).isShutdown();

        try
        {
            for (SocketThread th : socketThreads)
                th.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        streamExecutor_.shutdown();

        logger_.info("Waiting for in-progress requests to complete");
        callbacks.shutdown();
    }

    public void receive(Message message, String id)
    {
        if (logger_.isTraceEnabled())
            logger_.trace(FBUtilities.getBroadcastAddress() + " received " + message.getVerb()
                          + " from " + id + "@" + message.getFrom());

        message = SinkManager.processServerMessage(message, id);
        if (message == null)
            return;

        Runnable runnable = new MessageDeliveryTask(message, id);
        ExecutorService stage = StageManager.getStage(message.getMessageType());
        assert stage != null : "No stage for message type " + message.getMessageType();
        stage.execute(runnable);
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

    public static int getBits(int x, int p, int n)
    {
        return x >>> (p + 1) - n & ~(-1 << n);
    }

    public ByteBuffer constructStreamHeader(StreamHeader streamHeader, boolean compress, int version)
    {
        /*
        Setting up the protocol header. This is 4 bytes long
        represented as an integer. The first 2 bits indicate
        the serializer type. The 3rd bit indicates if compression
        is turned on or off. It is turned off by default. The 4th
        bit indicates if we are in streaming mode. It is turned off
        by default. The following 4 bits are reserved for future use.
        The next 8 bits indicate a version number. Remaining 15 bits
        are not used currently.
        */
        int header = 0;
        // Setting up the serializer bit
        header |= serializerType_.ordinal();
        // set compression bit.
        if ( compress )
            header |= 4;
        // set streaming bit
        header |= 8;
        // Setting up the version bit
        header |= (version << 8);
        /* Finished the protocol header setup */

        /* Adding the StreamHeader which contains the session Id along
         * with the pendingfile info for the stream.
         * | Session Id | Pending File Size | Pending File | Bool more files |
         * | No. of Pending files | Pending Files ... |
         */
        byte[] bytes;
        try
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            StreamHeader.serializer().serialize(streamHeader, buffer, version);
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

    public void incrementDroppedMessages(StorageService.Verb verb)
    {
        assert DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
        droppedMessages.get(verb).incrementAndGet();
    }

    private void logDroppedMessages()
    {
        boolean logTpstats = false;
        for (Map.Entry<StorageService.Verb, AtomicInteger> entry : droppedMessages.entrySet())
        {
            AtomicInteger dropped = entry.getValue();
            StorageService.Verb verb = entry.getKey();
            int recent = dropped.get() - lastDroppedInternal.get(verb);
            if (recent > 0)
            {
                logTpstats = true;
                logger_.info("{} {} messages dropped in last {}ms",
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
                    logger_.info("MessagingService shutting down server thread.");
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
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers_.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getCommandCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers_.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().cmdCon.getCompletedMesssages());
        return completedTasks;
    }

    public Map<String, Integer> getResponsePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<String, Integer>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers_.entrySet())
            pendingTasks.put(entry.getKey().getHostAddress(), entry.getValue().ackCon.getPendingMessages());
        return pendingTasks;
    }

    public Map<String, Long> getResponseCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<String, Long>();
        for (Map.Entry<InetAddress, OutboundTcpConnectionPool> entry : connectionManagers_.entrySet())
            completedTasks.put(entry.getKey().getHostAddress(), entry.getValue().ackCon.getCompletedMesssages());
        return completedTasks;
    }

    public static long getDefaultCallbackTimeout()
    {
        return DEFAULT_CALLBACK_TIMEOUT;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<StorageService.Verb, AtomicInteger> entry : droppedMessages.entrySet())
            map.put(entry.getKey().toString(), entry.getValue().get());
        return map;
    }

    public Map<String, Integer> getRecentlyDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<StorageService.Verb, AtomicInteger> entry : droppedMessages.entrySet())
        {
            StorageService.Verb verb = entry.getKey();
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
