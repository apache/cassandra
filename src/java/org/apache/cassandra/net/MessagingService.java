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

import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.io.SerializerType;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
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

    /** we preface every message with this number so the recipient can validate the sender is sane */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;
    /* Verb Handler for the Response */
    public static final String responseVerbHandler_ = "RESPONSE";

    /* This records all the results mapped by message Id */
    private static ExpiringMap<String, IAsyncCallback> callbackMap_;
    private static ExpiringMap<String, IAsyncResult> taskCompletionMap_;
    
    /* Lookup table for registering message handlers based on the verb. */
    private static Map<StorageService.Verb, IVerbHandler> verbHandlers_;

    /* Thread pool to handle deserialization of messages read from the socket. */
    private static ExecutorService messageDeserializerExecutor_;
    
    /* Thread pool to handle messaging write activities */
    private static ExecutorService streamExecutor_;
    
    private static NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool> connectionManagers_ = new NonBlockingHashMap<InetAddress, OutboundTcpConnectionPool>();
    
    private static Logger logger_ = LoggerFactory.getLogger(MessagingService.class);
    
    public static final MessagingService instance = new MessagingService();
    
    private SocketThread socketThread;

    public static int getVersion()
    {
        return version_;
    }

    public Object clone() throws CloneNotSupportedException
    {
        //Prevents the singleton from being cloned
        throw new CloneNotSupportedException();
    }

    protected MessagingService()
    {        
        verbHandlers_ = new HashMap<StorageService.Verb, IVerbHandler>();
        /*
         * Leave callbacks in the cachetable long enough that any related messages will arrive
         * before the callback is evicted from the table. The concurrency level is set at 128
         * which is the sum of the threads in the pool that adds shit into the table and the 
         * pool that retrives the callback from here.
        */
        callbackMap_ = new ExpiringMap<String, IAsyncCallback>( 2 * DatabaseDescriptor.getRpcTimeout() );
        taskCompletionMap_ = new ExpiringMap<String, IAsyncResult>( 2 * DatabaseDescriptor.getRpcTimeout() );

        // read executor puts messages to deserialize on this.
        messageDeserializerExecutor_ = new JMXEnabledThreadPoolExecutor(1,
                                                                        Runtime.getRuntime().availableProcessors(),
                                                                        Integer.MAX_VALUE,
                                                                        TimeUnit.SECONDS,
                                                                        new LinkedBlockingQueue<Runnable>(),
                                                                        new NamedThreadFactory("MESSAGE-DESERIALIZER-POOL"));

        streamExecutor_ = new JMXEnabledThreadPoolExecutor("MESSAGE-STREAMING-POOL");
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
        logger_.trace("Resetting pool for " + ep);
        getConnectionPool(ep).reset();
    }

    /**
     * Listen on the specified port.
     * @param localEp InetAddress whose port to listen on.
     */
    public void listen(InetAddress localEp) throws IOException
    {        
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        final ServerSocket ss = serverChannel.socket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort()));
        socketThread = new SocketThread(ss, "ACCEPT-" + localEp);
        socketThread.start();

    }

    public static OutboundTcpConnectionPool getConnectionPool(InetAddress to)
    {
        OutboundTcpConnectionPool cp = connectionManagers_.get(to);
        if (cp == null)
        {
            connectionManagers_.putIfAbsent(to, new OutboundTcpConnectionPool(to));
            cp = connectionManagers_.get(to);
        }
        return cp;
    }

    public static OutboundTcpConnection getConnection(InetAddress to, Message msg)
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

        // message sinks are a testing hook
        Message processedMessage = SinkManager.processClientMessageSink(message);
        if (processedMessage == null)
        {
            return;
        }

        // get pooled connection (really, connection queue)
        OutboundTcpConnection connection = getConnection(to, message);

        // pack message with header in a bytebuffer
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
        ByteBuffer buffer = packIt(data , false);

        // write it
        connection.write(buffer);
    }
    
    public IAsyncResult sendRR(Message message, InetAddress to)
    {
        IAsyncResult iar = new AsyncResult();
        taskCompletionMap_.put(message.getMessageId(), iar);
        sendOneWay(message, to);
        return iar;
    }
    
    /**
     * Stream a file from source to destination. This is highly optimized
     * to not hold any of the contents of the file in memory.
     * @param file name of file to stream.
     * @param startPosition position inside the file
     * @param endPosition
     * @param to endpoint to which we need to stream the file.
    */

    public void stream(String file, long startPosition, long endPosition, InetAddress from, InetAddress to)
    {
        /* Streaming asynchronously on streamExector_ threads. */
        Runnable streamingTask = new FileStreamTask(file, startPosition, endPosition, from, to);
        streamExecutor_.execute(streamingTask);
    }
    
    /** blocks until the processing pools are empty and done. */
    public static void waitFor() throws InterruptedException
    {
        while (!messageDeserializerExecutor_.isTerminated())
            messageDeserializerExecutor_.awaitTermination(5, TimeUnit.SECONDS);
        while (!streamExecutor_.isTerminated())
            streamExecutor_.awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void shutdown()
    {
        logger_.info("Shutting down MessageService...");

        try
        {
            instance.socketThread.close();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        messageDeserializerExecutor_.shutdownNow();
        streamExecutor_.shutdownNow();

        /* shut down the cachetables */
        taskCompletionMap_.shutdown();
        callbackMap_.shutdown();

        logger_.info("Shutdown complete (no further commands will be processed)");
    }

    public static void receive(Message message)
    {
        Runnable runnable = new MessageDeliveryTask(message);

        ExecutorService stage = StageManager.getStage(message.getMessageType());
        if (stage == null)
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Running " + message.getMessageType() + " on default stage");
            messageDeserializerExecutor_.execute(runnable);
        }
        else
        {
            stage.execute(runnable);
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

    public static ExecutorService getDeserializationExecutor()
    {
        return messageDeserializerExecutor_;
    }

    public static void validateMagic(int magic) throws IOException
    {
        if (magic != PROTOCOL_MAGIC)
            throw new IOException("invalid protocol header");
    }
    
    public static boolean isEqual(byte digestA[], byte digestB[])
    {
        return MessageDigest.isEqual(digestA, digestB);
    }

    public static int getBits(int x, int p, int n)
    {
        return x >>> (p + 1) - n & ~(-1 << n);
    }
        
    public static ByteBuffer packIt(byte[] bytes, boolean compress)
    {
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
        int header = 0;
        // Setting up the serializer bit
        header |= serializerType_.ordinal();
        // set compression bit.
        if (compress)
            header |= 4;
        // Setting up the version bit
        header |= (version_ << 8);

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + bytes.length);
        buffer.putInt(PROTOCOL_MAGIC);
        buffer.putInt(header);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }
        
    public static ByteBuffer constructStreamHeader(boolean compress)
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
        header |= (version_ << 8);
        /* Finished the protocol header setup */

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4);
        buffer.putInt(PROTOCOL_MAGIC);
        buffer.putInt(header);
        buffer.flip();
        return buffer;
    }
    
    private class SocketThread extends Thread
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
}
