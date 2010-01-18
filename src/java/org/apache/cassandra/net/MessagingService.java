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

import org.apache.cassandra.cache.ICachetable;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.io.SerializerType;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.utils.*;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessagingService implements IFailureDetectionEventListener
{
    private static int version_ = 1;
    //TODO: make this parameter dynamic somehow.  Not sure if config is appropriate.
    private static SerializerType serializerType_ = SerializerType.BINARY;
    
    private static byte[] protocol_ = new byte[16];
    /* Verb Handler for the Response */
    public static final String responseVerbHandler_ = "RESPONSE";

    /* This records all the results mapped by message Id */
    private static ICachetable<String, IAsyncCallback> callbackMap_;
    private static ICachetable<String, IAsyncResult> taskCompletionMap_;
    
    /* List of sockets we are listening on */
    private static Map<InetAddress, SelectionKey> listenSockets_ = new HashMap<InetAddress, SelectionKey>();

    /* List of UdpConnections we are listening on */
    private static Map<InetAddress, UdpConnection> udpConnections_ = new HashMap<InetAddress, UdpConnection>();
    
    /* Lookup table for registering message handlers based on the verb. */
    private static Map<String, IVerbHandler> verbHandlers_;

    /* Thread pool to handle messaging read activities of Socket and default stage */
    private static ExecutorService messageReadExecutor_;
    
    /* Thread pool to handle deserialization of messages read from the socket. */
    private static ExecutorService messageDeserializerExecutor_;
    
    /* Thread pool to handle messaging write activities */
    private static ExecutorService streamExecutor_;
    
    private static NonBlockingHashMap<String, TcpConnectionManager> connectionManagers_ = new NonBlockingHashMap<String, TcpConnectionManager>();
    
    private static volatile boolean bShutdown_ = false;
    
    private static Logger logger_ = Logger.getLogger(MessagingService.class);
    
    private static volatile MessagingService messagingService_ = new MessagingService();

    public static int getVersion()
    {
        return version_;
    }

    public static MessagingService instance()
    {   
    	if ( bShutdown_ )
    	{
            synchronized (MessagingService.class)
            {
                if ( bShutdown_ )
                {
            		messagingService_ = new MessagingService();
            		bShutdown_ = false;
                }
            }
    	}
        return messagingService_;
    }
    
    public Object clone() throws CloneNotSupportedException
    {
        //Prevents the singleton from being cloned
        throw new CloneNotSupportedException();
    }

    protected MessagingService()
    {        
        verbHandlers_ = new HashMap<String, IVerbHandler>();
        /*
         * Leave callbacks in the cachetable long enough that any related messages will arrive
         * before the callback is evicted from the table. The concurrency level is set at 128
         * which is the sum of the threads in the pool that adds shit into the table and the 
         * pool that retrives the callback from here.
        */
        callbackMap_ = new Cachetable<String, IAsyncCallback>( 2 * DatabaseDescriptor.getRpcTimeout() );
        taskCompletionMap_ = new Cachetable<String, IAsyncResult>( 2 * DatabaseDescriptor.getRpcTimeout() );        

        // read executor will have one runnable enqueued per connection with stuff to read on it,
        // so there is no need to make it bounded, and one thread should be plenty.
        messageReadExecutor_ = new JMXEnabledThreadPoolExecutor("MS-CONNECTION-READ-POOL");

        // read executor puts messages to deserialize on this.
        messageDeserializerExecutor_ = new JMXEnabledThreadPoolExecutor(1,
                                                                        Runtime.getRuntime().availableProcessors(),
                                                                        Integer.MAX_VALUE,
                                                                        TimeUnit.SECONDS,
                                                                        new LinkedBlockingQueue<Runnable>(),
                                                                        new NamedThreadFactory("MESSAGE-DESERIALIZER-POOL"));

        streamExecutor_ = new JMXEnabledThreadPoolExecutor("MESSAGE-STREAMING-POOL");
                
        protocol_ = hash("MD5", "FB-MESSAGING".getBytes());        
        /* register the response verb handler */
        registerVerbHandlers(MessagingService.responseVerbHandler_, new ResponseVerbHandler());
    }
    
    public byte[] hash(String type, byte data[])
    {
        byte result[];
        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            result = messageDigest.digest(data);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }

    /** called by failure detection code to notify that housekeeping should be performed on downed sockets. */
    public void convict(InetAddress ep)
    {
        logger_.debug("Canceling pool for " + ep);
        getConnectionPool(FBUtilities.getLocalAddress(), ep).reset();
    }

    /**
     * Listen on the specified port.
     * @param localEp InetAddress whose port to listen on.
     */
    public void listen(InetAddress localEp) throws IOException
    {        
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        ServerSocket ss = serverChannel.socket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort()));
        serverChannel.configureBlocking(false);
        
        SelectionKeyHandler handler = new TcpConnectionHandler(localEp);

        SelectionKey key = SelectorManager.getSelectorManager().register(serverChannel, handler, SelectionKey.OP_ACCEPT);          
        listenSockets_.put(localEp, key);
        FailureDetector.instance.registerFailureDetectionEventListener(this);
    }
    
    /**
     * Listen on the specified port.
     * @param localEp InetAddress whose port to listen on.
     */
    public void listenUDP(InetAddress localEp)
    {
        UdpConnection connection = new UdpConnection();
        if (logger_.isDebugEnabled())
          logger_.debug("Starting to listen on " + localEp);
        try
        {
            connection.init(localEp);
            udpConnections_.put(localEp, connection);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    public static TcpConnectionManager getConnectionPool(InetAddress from, InetAddress to)
    {
        String key = from + ":" + to;
        TcpConnectionManager cp = connectionManagers_.get(key);
        if (cp == null)
        {
            connectionManagers_.putIfAbsent(key, new TcpConnectionManager(from, to));
            cp = connectionManagers_.get(key);
        }
        return cp;
    }

    public static TcpConnection getConnection(InetAddress from, InetAddress to, Message msg) throws IOException
    {
        return getConnectionPool(from, to).getConnection(msg);
    }
        
    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     * @param type name of the verb.
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(String type, IVerbHandler verbHandler)
    {
    	assert !verbHandlers_.containsKey(type);
    	verbHandlers_.put(type, verbHandler);
    }
        
    /**
     * This method returns the verb handler associated with the registered
     * verb. If no handler has been registered then null is returned.
     * @param type for which the verb handler is sought
     * @return a reference to IVerbHandler which is the handler for the specified verb
     */
    public IVerbHandler getVerbHandler(String type)
    {
        return verbHandlers_.get(type);
    }

    /**
     * Send a message to a given endpoint.
     * @param message message to be sent.
     * @param to endpoint to which the message needs to be sent
     * @return an reference to an IAsyncResult which can be queried for the
     * response
     */
    public String sendRR(Message message, InetAddress[] to, IAsyncCallback cb)
    {
        String messageId = message.getMessageId();
        addCallback(cb, messageId);
        for (InetAddress endpoint : to)
        {
            sendOneWay(message, endpoint);
        }
        return messageId;
    }

    public void addCallback(IAsyncCallback cb, String messageId)
    {
        callbackMap_.put(messageId, cb);
    }

    /**
     * Send a message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * @param message message to be sent.
     * @param to endpoint to which the message needs to be sent
     * @param cb callback interface which is used to pass the responses or
     *           suggest that a timeout occurred to the invoker of the send().
     *           suggest that a timeout occurred to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public String sendRR(Message message, InetAddress to, IAsyncCallback cb)
    {        
        String messageId = message.getMessageId();
        addCallback(cb, messageId);
        sendOneWay(message, to);
        return messageId;
    }

    /**
     * Send a message to a given endpoint. The ith element in the <code>messages</code>
     * array is sent to the ith element in the <code>to</code> array.This method assumes
     * there is a one-one mapping between the <code>messages</code> array and
     * the <code>to</code> array. Otherwise an  IllegalArgumentException will be thrown.
     * This method also informs the MessagingService to wait for at least
     * <code>howManyResults</code> responses to determine success of failure.
     * @param messages messages to be sent.
     * @param to endpoints to which the message needs to be sent
     * @param cb callback interface which is used to pass the responses or
     *           suggest that a timeout occured to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public String sendRR(Message[] messages, InetAddress[] to, IAsyncCallback cb)
    {
        if ( messages.length != to.length )
        {
            throw new IllegalArgumentException("Number of messages and the number of endpoints need to be same.");
        }
        String groupId = GuidGenerator.guid();
        addCallback(cb, groupId);
        for ( int i = 0; i < messages.length; ++i )
        {
            messages[i].setMessageId(groupId);
            sendOneWay(messages[i], to[i]);
        }
        return groupId;
    } 
    
    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     * @param message messages to be sent.
     * @param to endpoint to which the message needs to be sent
     */
    public void sendOneWay(Message message, InetAddress to)
    {
        // do local deliveries
        if ( message.getFrom().equals(to) )
        {
            MessagingService.receive(message);
            return;
        }

        Message processedMessage = SinkManager.processClientMessageSink(message);
        if (processedMessage == null)
        {
            return;
        }

        byte[] data;
        try
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            Message.serializer().serialize(message, buffer);
            data = buffer.getData();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        assert data.length > 0;
        ByteBuffer buffer = packIt(data , false, false);

        TcpConnection connection = null;
        try
        {
            connection = MessagingService.getConnection(processedMessage.getFrom(), to, message);
            connection.write(buffer);
        }
        catch (IOException e)
        {
            if (connection != null)
            {
                connection.errorClose();
            }
            logger_.error("unexpected error writing " + message, e);
        }
    }
    
    public IAsyncResult sendRR(Message message, InetAddress to)
    {
        IAsyncResult iar = new AsyncResult();
        taskCompletionMap_.put(message.getMessageId(), iar);
        sendOneWay(message, to);
        return iar;
    }
    
    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     * @param message messages to be sent.
     * @param to endpoint to which the message needs to be sent
     */
    public void sendUdpOneWay(Message message, InetAddress to)
    {
        if (message.getFrom().equals(to)) {
            MessagingService.receive(message);
            return;
        }
        
        UdpConnection connection = null;
        try
        {
            connection = new UdpConnection(); 
            connection.init();            
            connection.write(message, to);            
        }            
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if ( connection != null )
                connection.close();
        }
    }
    /**
     * Stream a file from source to destination. This is highly optimized
     * to not hold any of the contents of the file in memory.
     * @param file name of file to stream.
     * @param startPosition position inside the file
     * @param total number of bytes to stream
     * @param to endpoint to which we need to stream the file.
    */

    public void stream(String file, long startPosition, long total, InetAddress from, InetAddress to)
    {
        /* Streaming asynchronously on streamExector_ threads. */
        Runnable streamingTask = new FileStreamTask(file, startPosition, total, from, to);
        streamExecutor_.execute(streamingTask);
    }

    public static void shutdown()
    {
        logger_.info("Shutting down ...");
        synchronized (MessagingService.class)
        {
            FailureDetector.instance.unregisterFailureDetectionEventListener(MessagingService.instance());
            /* Stop listening on any TCP socket */
            for (SelectionKey skey : listenSockets_.values())
            {
                skey.cancel();
                try
                {
                    skey.channel().close();
                }
                catch (IOException e) {}
            }
            listenSockets_.clear();

            /* Stop listening on any UDP ports. */
            for (UdpConnection con : udpConnections_.values())
            {
                con.close();
            }
            udpConnections_.clear();

            /* Shutdown the threads in the EventQueue's */
            messageReadExecutor_.shutdownNow();
            messageDeserializerExecutor_.shutdownNow();
            streamExecutor_.shutdownNow();

            StageManager.shutdown();
            
            /* shut down the cachetables */
            taskCompletionMap_.shutdown();
            callbackMap_.shutdown();

            /* Interrupt the selector manager thread */
            SelectorManager.getSelectorManager().interrupt();

            connectionManagers_.clear();
            verbHandlers_.clear();
            bShutdown_ = true;
        }
        logger_.info("Shutdown invocation complete.");
    }

    public static void receive(Message message)
    {        
        enqueueRunnable(message.getMessageType(), new MessageDeliveryTask(message));
    }

    private static void enqueueRunnable(String stageName, Runnable runnable){
        
        ExecutorService stage = StageManager.getStage(stageName);
        
        if ( stage != null )
        {
            stage.execute(runnable);
        } 
        else
        {
            logger_.warn("Running on default stage - beware");
            messageDeserializerExecutor_.execute(runnable);
        }
    }    
    
    public static IAsyncCallback getRegisteredCallback(String key)
    {
        return callbackMap_.get(key);
    }
    
    public static void removeRegisteredCallback(String key)
    {
        callbackMap_.remove(key);
    }
    
    public static IAsyncResult getAsyncResult(String key)
    {
        return taskCompletionMap_.remove(key);
    }

    public static ExecutorService getReadExecutor()
    {
        return messageReadExecutor_;
    }

    public static ExecutorService getDeserializationExecutor()
    {
        return messageDeserializerExecutor_;
    }

    public static boolean isProtocolValid(byte[] protocol)
    {
        return isEqual(protocol_, protocol);
    }
    
    public static boolean isEqual(byte digestA[], byte digestB[])
    {
        return MessageDigest.isEqual(digestA, digestB);
    }

    public static int getBits(int x, int p, int n)
    {
        return x >>> (p + 1) - n & ~(-1 << n);
    }
        
    public static ByteBuffer packIt(byte[] bytes, boolean compress, boolean stream)
    {
        byte[] size = FBUtilities.toByteArray(bytes.length);
        /* 
             Setting up the protocol header. This is 4 bytes long
             represented as an integer. The first 2 bits indicate
             the serializer type. The 3rd bit indicates if compression
             is turned on or off. It is turned off by default. The 4th
             bit indicates if we are in streaming mode. It is turned off
             by default. The 5th-8th bits are reserved for future use.
             The next 8 bits indicate a version number. Remaining 15 bits 
             are not used currently.            
        */
        int n = 0;
        // Setting up the serializer bit
        n |= serializerType_.ordinal();
        // set compression bit.
        if ( compress )
            n |= 4;
        
        // set streaming bit
        if ( stream )
            n |= 8;
        
        // Setting up the version bit
        n |= (version_ << 8);               
        /* Finished the protocol header setup */

        byte[] header = FBUtilities.toByteArray(n);
        ByteBuffer buffer = ByteBuffer.allocate(16 + header.length + size.length + bytes.length);
        buffer.put(protocol_);
        buffer.put(header);
        buffer.put(size);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }
        
    public static ByteBuffer constructStreamHeader(boolean compress, boolean stream)
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
        int n = 0;
        // Setting up the serializer bit
        n |= serializerType_.ordinal();
        // set compression bit.
        if ( compress )
            n |= 4;
       
        // set streaming bit
        if ( stream )
            n |= 8;
       
        // Setting up the version bit 
        n |= (version_ << 8);              
        /* Finished the protocol header setup */

        byte[] header = FBUtilities.toByteArray(n);
        ByteBuffer buffer = ByteBuffer.allocate(16 + header.length);
        buffer.put(protocol_);
        buffer.put(header);
        buffer.flip();
        return buffer;
    }
}
