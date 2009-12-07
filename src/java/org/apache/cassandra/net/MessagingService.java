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
import org.apache.cassandra.net.io.SerializerType;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.utils.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class MessagingService
{
    private static int version_ = 1;
    //TODO: make this parameter dynamic somehow.  Not sure if config is appropriate.
    private static SerializerType serializerType_ = SerializerType.BINARY;
    
    private static byte[] protocol_ = new byte[16];
    /* Verb Handler for the Response */
    public static final String responseVerbHandler_ = "RESPONSE";
    /* Stage for responses. */
    public static final String responseStage_ = "RESPONSE-STAGE";

    private enum ReservedVerbs_ {
    };
    
    private static Map<String, String> reservedVerbs_ = new Hashtable<String, String>();
    
    /* This records all the results mapped by message Id */
    private static ICachetable<String, IAsyncCallback> callbackMap_;
    private static ICachetable<String, IAsyncResult> taskCompletionMap_;
    
    /* Manages the table of endpoints it is listening on */
    private static Set<InetAddress> endPoints_;
    
    /* List of sockets we are listening on */
    private static Map<InetAddress, SelectionKey> listenSockets_ = new HashMap<InetAddress, SelectionKey>();

    /* List of UdpConnections we are listening on */
    private static Map<InetAddress, UdpConnection> udpConnections_ = new HashMap<InetAddress, UdpConnection>();
    
    /* Lookup table for registering message handlers based on the verb. */
    private static Map<String, IVerbHandler> verbHandlers_;

    /* Thread pool to handle messaging read activities of Socket and default stage */
    private static ExecutorService messageDeserializationExecutor_;
    
    /* Thread pool to handle deserialization of messages read from the socket. */
    private static ExecutorService messageDeserializerExecutor_;
    
    /* Thread pool to handle messaging write activities */
    private static ExecutorService streamExecutor_;
    
    private final static ReentrantLock lock_ = new ReentrantLock();
    private static Map<String, TcpConnectionManager> poolTable_ = new Hashtable<String, TcpConnectionManager>();
    
    private static volatile boolean bShutdown_ = false;
    
    private static Logger logger_ = Logger.getLogger(MessagingService.class);
    
    private static volatile MessagingService messagingService_ = new MessagingService();

    private static final int MESSAGE_DESERIALIZE_THREADS = 4;

    public static int getVersion()
    {
        return version_;
    }

    public static MessagingService instance()
    {   
    	if ( bShutdown_ )
    	{
            lock_.lock();
            try
            {
                if ( bShutdown_ )
                {
            		messagingService_ = new MessagingService();
            		bShutdown_ = false;
                }
            }
            finally
            {
                lock_.unlock();
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
        for ( ReservedVerbs_ verbs : ReservedVerbs_.values() )
        {
            reservedVerbs_.put(verbs.toString(), verbs.toString());
        }
        verbHandlers_ = new HashMap<String, IVerbHandler>();        
        endPoints_ = new HashSet<InetAddress>();
        /*
         * Leave callbacks in the cachetable long enough that any related messages will arrive
         * before the callback is evicted from the table. The concurrency level is set at 128
         * which is the sum of the threads in the pool that adds shit into the table and the 
         * pool that retrives the callback from here.
        */ 
        int maxSize = MESSAGE_DESERIALIZE_THREADS;
        callbackMap_ = new Cachetable<String, IAsyncCallback>( 2 * DatabaseDescriptor.getRpcTimeout() );
        taskCompletionMap_ = new Cachetable<String, IAsyncResult>( 2 * DatabaseDescriptor.getRpcTimeout() );        
        
        messageDeserializationExecutor_ = new DebuggableThreadPoolExecutor( maxSize,
                maxSize,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("MESSAGING-SERVICE-POOL")
                );

        messageDeserializerExecutor_ = new DebuggableThreadPoolExecutor( maxSize,
                maxSize,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("MESSAGE-DESERIALIZER-POOL")
                ); 
        
        streamExecutor_ = new DebuggableThreadPoolExecutor("MESSAGE-STREAMING-POOL");
                
        protocol_ = hash(HashingSchemes.MD5, "FB-MESSAGING".getBytes());        
        /* register the response verb handler */
        registerVerbHandlers(MessagingService.responseVerbHandler_, new ResponseVerbHandler());
        /* register stage for response */
        StageManager.registerStage(MessagingService.responseStage_, new MultiThreadedStage("RESPONSE-STAGE", maxSize) );
    }
    
    public byte[] hash(String type, byte data[])
    {
        byte result[] = null;
        try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            result = messageDigest.digest(data);
        }
        catch(Exception e)
        {
            if (logger_.isDebugEnabled())
                logger_.debug(LogUtil.throwableToString(e));
        }
        return result;
    }
    
    /**
     * Listen on the specified port.
     * @param localEp InetAddress whose port to listen on.
     */
    public void listen(InetAddress localEp) throws IOException
    {        
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        ServerSocket ss = serverChannel.socket();
        ss.bind(new InetSocketAddress(localEp, DatabaseDescriptor.getStoragePort()));
        serverChannel.configureBlocking(false);
        
        SelectionKeyHandler handler = new TcpConnectionHandler(localEp);

        SelectionKey key = SelectorManager.getSelectorManager().register(serverChannel, handler, SelectionKey.OP_ACCEPT);          
        endPoints_.add(localEp);            
        listenSockets_.put(localEp, key);             
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
            endPoints_.add(localEp);
            udpConnections_.put(localEp, connection);
        }
        catch ( IOException e )
        {
            logger_.warn(LogUtil.throwableToString(e));
        }
    }
    
    public static TcpConnectionManager getConnectionPool(InetAddress from, InetAddress to)
    {
        String key = from + ":" + to;
        TcpConnectionManager cp = poolTable_.get(key);
        if( cp == null )
        {
            lock_.lock();
            try
            {
                cp = poolTable_.get(key);
                if (cp == null )
                {
                    cp = new TcpConnectionManager(from, to);
                    poolTable_.put(key, cp);
                }
            }
            finally
            {
                lock_.unlock();
            }
        }
        return cp;
    }

    public static TcpConnection getConnection(InetAddress from, InetAddress to, Message msg) throws IOException
    {
        return getConnectionPool(from, to).getConnection(msg);
    }
    
    private void checkForReservedVerb(String type)
    {
    	if ( reservedVerbs_.get(type) != null && verbHandlers_.get(type) != null )
    	{
    		throw new IllegalArgumentException( type + " is a reserved verb handler. Scram!");
    	}
    }     
    
    /**
     * Register a verb and the corresponding verb handler with the
     * Messaging Service.
     * @param type name of the verb.
     * @param verbHandler handler for the specified verb
     */
    public void registerVerbHandlers(String type, IVerbHandler verbHandler)
    {
    	checkForReservedVerb(type);
    	verbHandlers_.put(type, verbHandler);
    }
    
    /**
     * Deregister all verbhandlers corresponding to localEndPoint.
     * @param localEndPoint
     */
    public void deregisterAllVerbHandlers(InetAddress localEndPoint)
    {
        Iterator keys = verbHandlers_.keySet().iterator();
        String key = null;
        
        /*
         * endpoint specific verbhandlers can be distinguished because
         * their key's contain the name of the endpoint. 
         */
        while(keys.hasNext())
        {
            key = (String)keys.next();
            if (key.contains(localEndPoint.toString()))
                keys.remove();
        }
    }
    
    /**
     * Deregister a verbhandler corresponding to the verb from the
     * Messaging Service.
     * @param type name of the verb.
     */
    public void deregisterVerbHandlers(String type)
    {
        verbHandlers_.remove(type);
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
        for ( int i = 0; i < to.length; ++i )
        {
            sendOneWay(message, to[i]);
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

        TcpConnection connection = null;
        try
        {
            Message processedMessage = SinkManager.processClientMessageSink(message);
            if (processedMessage == null)
            {
                return;
            }
            connection = MessagingService.getConnection(processedMessage.getFrom(), to, message);
            connection.write(message);
        }
        catch (SocketException se)
        {
            // Shutting down the entire pool. May be too conservative an approach.
            MessagingService.getConnectionPool(message.getFrom(), to).shutdown();
            logger_.error("socket error writing to " + to, se);
        }
        catch (IOException e)
        {
            if (connection != null)
            {
                connection.errorClose();
            }
            logger_.error("unexpected error writing " + message, e);
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
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
        catch ( IOException e )
        {               
            logger_.warn(LogUtil.throwableToString(e));
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
            messageDeserializationExecutor_.shutdownNow();
            messageDeserializerExecutor_.shutdownNow();
            streamExecutor_.shutdownNow();

            StageManager.shutdown();
            
            /* shut down the cachetables */
            taskCompletionMap_.shutdown();
            callbackMap_.shutdown();

            /* Interrupt the selector manager thread */
            SelectorManager.getSelectorManager().interrupt();

            poolTable_.clear();
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
        
        IStage stage = StageManager.getStage(stageName);   
        
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
        return messageDeserializationExecutor_;
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

    public static byte[] toByteArray(int i)
    {
        byte bytes[] = new byte[4];
        bytes[0] = (byte)(i >>> 24 & 0xff);
        bytes[1] = (byte)(i >>> 16 & 0xff);
        bytes[2] = (byte)(i >>> 8 & 0xff);
        bytes[3] = (byte)(i & 0xff);
        return bytes;
    }
    
    public static byte[] toByteArray(short s)
    {
        byte bytes[] = new byte[2];
        bytes[0] = (byte)(s >>> 8 & 0xff);
        bytes[1] = (byte)(s & 0xff);
        return bytes;
    }
    
    public static short byteArrayToShort(byte bytes[])
    {
        return byteArrayToShort(bytes, 0);
    }
    
    public static short byteArrayToShort(byte bytes[], int offset)
    {
        if(bytes.length - offset < 2)
            throw new IllegalArgumentException("A short must be 2 bytes in size.");
        short n = 0;
        for(int i = 0; i < 2; i++)
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xff;
        }

        return n;
    }

    public static int getBits(int x, int p, int n)
    {
        return x >>> (p + 1) - n & ~(-1 << n);
    }
    
    public static int byteArrayToInt(byte bytes[])
    {
        return byteArrayToInt(bytes, 0);
    }

    public static int byteArrayToInt(byte bytes[], int offset)
    {
        if(bytes.length - offset < 4)
            throw new IllegalArgumentException("An integer must be 4 bytes in size.");
        int n = 0;
        for(int i = 0; i < 4; i++)
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xff;
        }

        return n;
    }
    
    public static ByteBuffer packIt(byte[] bytes, boolean compress, boolean stream)
    {
        byte[] size = toByteArray(bytes.length);
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
               
        byte[] header = toByteArray(n);
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
              
        byte[] header = toByteArray(n);
        ByteBuffer buffer = ByteBuffer.allocate(16 + header.length);
        buffer.put(protocol_);
        buffer.put(header);
        buffer.flip();
        return buffer;
    }
}
