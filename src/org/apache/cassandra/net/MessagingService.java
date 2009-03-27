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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.channels.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.net.io.*;
import org.apache.cassandra.utils.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.xml.bind.*;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.IStage;
import org.apache.cassandra.concurrent.MultiThreadedStage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.http.HttpConnectionHandler;
import org.apache.cassandra.net.io.SerializerType;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.utils.Cachetable;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.HashingSchemes;
import org.apache.cassandra.utils.ICachetable;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class MessagingService implements IMessagingService, MessagingServiceMBean
{
    private static boolean debugOn_ = false;   
    
    private static int version_ = 1;
    //TODO: make this parameter dynamic somehow.  Not sure if config is appropriate.
    private static SerializerType serializerType_ = SerializerType.BINARY;
    
    private static byte[] protocol_ = new byte[16];
    /* Verb Handler for the Response */
    public static final String responseVerbHandler_ = "RESPONSE";
    /* Stage for responses. */
    public static final String responseStage_ = "RESPONSE-STAGE";
    private enum ReservedVerbs_ {RESPONSE};
    
    private static Map<String, String> reservedVerbs_ = new Hashtable<String, String>();
    /* Indicate if we are currently streaming data to another node or receiving streaming data */
    private static AtomicBoolean isStreaming_ = new AtomicBoolean(false);
    
    /* This records all the results mapped by message Id */
    private static ICachetable<String, IAsyncCallback> callbackMap_;
    private static ICachetable<String, IAsyncResult> taskCompletionMap_;
    
    /* Manages the table of endpoints it is listening on */
    private static Set<EndPoint> endPoints_;
    
    /* List of sockets we are listening on */
    private static Map<EndPoint, SelectionKey> listenSockets_ = new HashMap<EndPoint, SelectionKey>();
    
    /* Lookup table for registering message handlers based on the verb. */
    private static Map<String, IVerbHandler> verbHandlers_;
    
    private static Map<String, MulticastSocket> mCastMembership_ = new HashMap<String, MulticastSocket>();
    
    /* Thread pool to handle messaging read activities of Socket and default stage */
    private static ExecutorService messageDeserializationExecutor_;
    
    /* Thread pool to handle messaging write activities */
    private static ExecutorService messageSerializerExecutor_;
    
    /* Thread pool to handle deserialization of messages read from the socket. */
    private static ExecutorService messageDeserializerExecutor_;
    
    /* Thread pool to handle messaging write activities */
    private static ExecutorService streamExecutor_;
    
    private final static ReentrantLock lock_ = new ReentrantLock();
    private static Map<String, TcpConnectionManager> poolTable_ = new Hashtable<String, TcpConnectionManager>();
    
    private static boolean bShutdown_ = false;
    
    private static Logger logger_ = Logger.getLogger(MessagingService.class);
    
    private static IMessagingService messagingService_ = new MessagingService();
    
    public static boolean isDebugOn()
    {
        return debugOn_;
    }
    
    public static void debugOn(boolean on)
    {
        debugOn_ = on;
    }
    
    public static SerializerType getSerializerType()
    {
        return serializerType_;
    }
    
    public synchronized static void serializerType(String type)
    { 
        if ( type.equalsIgnoreCase("binary") )
        {
            serializerType_ = SerializerType.BINARY;
        }
        else if ( type.equalsIgnoreCase("java") )
        {
            serializerType_ = SerializerType.JAVA;
        }
        else if ( type.equalsIgnoreCase("xml") )
        {
            serializerType_ = SerializerType.XML;
        }
    }
    
    public static int getVersion()
    {
        return version_;
    }
    
    public static void setVersion(int version)
    {
        version_ = version;
    }
    
    public static IMessagingService getMessagingInstance()
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
        endPoints_ = new HashSet<EndPoint>();
        /*
         * Leave callbacks in the cachetable long enough that any related messages will arrive
         * before the callback is evicted from the table. The concurrency level is set at 128
         * which is the sum of the threads in the pool that adds shit into the table and the 
         * pool that retrives the callback from here.
        */ 
        int maxSize = MessagingConfig.getMessagingThreadCount();
        callbackMap_ = new Cachetable<String, IAsyncCallback>( 2 * DatabaseDescriptor.getRpcTimeout() );
        taskCompletionMap_ = new Cachetable<String, IAsyncResult>( 2 * DatabaseDescriptor.getRpcTimeout() );        
        
        messageDeserializationExecutor_ = new DebuggableThreadPoolExecutor( maxSize,
                maxSize,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("MESSAGING-SERVICE-POOL")
                );
                
        messageSerializerExecutor_ = new DebuggableThreadPoolExecutor( maxSize,
                maxSize,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("MESSAGE-SERIALIZER-POOL")
                ); 
        
        messageDeserializerExecutor_ = new DebuggableThreadPoolExecutor( maxSize,
                maxSize,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("MESSAGE-DESERIALIZER-POOL")
                ); 
        
        streamExecutor_ = new DebuggableThreadPoolExecutor( 1,
                1,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("MESSAGE-STREAMING-POOL")
                ); 
                
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
            LogUtil.getLogger(MessagingService.class.getName()).debug(LogUtil.throwableToString(e));
        }
        return result;
    }
    
    public long getMessagingSerializerTaskCount()
    {
        DebuggableThreadPoolExecutor dstp = (DebuggableThreadPoolExecutor)messageSerializerExecutor_;        
        return dstp.getTaskCount() - dstp.getCompletedTaskCount();
    }
    
    public long getMessagingReceiverTaskCount()
    {
        DebuggableThreadPoolExecutor dstp = (DebuggableThreadPoolExecutor)messageDeserializationExecutor_;        
        return dstp.getTaskCount() - dstp.getCompletedTaskCount(); 
    }
    
    public void listen(EndPoint localEp, boolean isHttp) throws IOException
    {        
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        ServerSocket ss = serverChannel.socket();            
        ss.bind(localEp.getInetAddress());
        serverChannel.configureBlocking(false);
        
        SelectionKeyHandler handler = null;
        if ( isHttp )
        {                
            handler = new HttpConnectionHandler();
        }
        else
        {
            handler = new TcpConnectionHandler(localEp);
        }
        
        SelectionKey key = SelectorManager.getSelectorManager().register(serverChannel, handler, SelectionKey.OP_ACCEPT);          
        endPoints_.add(localEp);            
        listenSockets_.put(localEp, key);             
    }
    
    public void listenUDP(EndPoint localEp)
    {
        UdpConnection connection = new UdpConnection();
        logger_.debug("Starting to listen on " + localEp);
        try
        {
            connection.init(localEp.getPort());
            endPoints_.add(localEp);     
        }
        catch ( IOException e )
        {
            logger_.warn(LogUtil.throwableToString(e));
        }
    }
    
    public static TcpConnectionManager getConnectionPool(EndPoint from, EndPoint to)
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
                    cp = new TcpConnectionManager(MessagingConfig.getConnectionPoolInitialSize(), 
                            MessagingConfig.getConnectionPoolGrowthFactor(), 
                            MessagingConfig.getConnectionPoolMaxSize(), from, to);
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

    public static ConnectionStatistics[] getPoolStatistics()
    {
        Set<ConnectionStatistics> stats = new HashSet<ConnectionStatistics>();        
        Iterator<TcpConnectionManager> it = poolTable_.values().iterator();
        while ( it.hasNext() )
        {
            TcpConnectionManager cp = it.next();
            ConnectionStatistics cs = new ConnectionStatistics(cp.getLocalEndPoint(), cp.getRemoteEndPoint(), cp.getPoolSize(), cp.getConnectionsInUse());
            stats.add( cs );
        }
        return stats.toArray(new ConnectionStatistics[0]);
    }
    
    public static TcpConnection getConnection(EndPoint from, EndPoint to) throws IOException
    {
        return getConnectionPool(from, to).getConnection();
    }
    
    private void checkForReservedVerb(String type)
    {
    	if ( reservedVerbs_.get(type) != null && verbHandlers_.get(type) != null )
    	{
    		throw new IllegalArgumentException( type + " is a reserved verb handler. Scram!");
    	}
    }     
    
    public void registerVerbHandlers(String type, IVerbHandler verbHandler)
    {
    	checkForReservedVerb(type);
    	verbHandlers_.put(type, verbHandler);
    }
    
    public void deregisterAllVerbHandlers(EndPoint localEndPoint)
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
            if(-1 != key.indexOf(localEndPoint.toString()))
                keys.remove();
        }
    }
    
    public void deregisterVerbHandlers(String type)
    {
        verbHandlers_.remove(type);
    }

    public IVerbHandler getVerbHandler(String type)
    {
        IVerbHandler handler = (IVerbHandler)verbHandlers_.get(type);
        return handler;
    }

    public String sendRR(Message message, EndPoint[] to, IAsyncCallback cb)
    {
        String messageId = message.getMessageId();                        
        callbackMap_.put(messageId, cb);
        for ( int i = 0; i < to.length; ++i )
        {
            sendOneWay(message, to[i]);
        }
        return messageId;
    }
    
    public String sendRR(Message message, EndPoint to, IAsyncCallback cb)
    {        
        String messageId = message.getMessageId();
        callbackMap_.put(messageId, cb);
        sendOneWay(message, to);
        return messageId;
    }

    public String sendRR(Message[] messages, EndPoint[] to, IAsyncCallback cb)
    {
        if ( messages.length != to.length )
        {
            throw new IllegalArgumentException("Number of messages and the number of endpoints need to be same.");
        }
        String groupId = GuidGenerator.guid();
        callbackMap_.put(groupId, cb);
        for ( int i = 0; i < messages.length; ++i )
        {
            messages[i].setMessageId(groupId);
            sendOneWay(messages[i], to[i]);
        }
        return groupId;
    } 
    
    public IAsyncResult sendRR(Message[] messages, EndPoint[] to)
    {
        if ( messages.length != to.length )
        {
            throw new IllegalArgumentException("Number of messages and the number of endpoints need to be same.");
        }
        
        IAsyncResult iar = new MultiAsyncResult(messages.length);
        String groupId = GuidGenerator.guid();
        taskCompletionMap_.put(groupId, iar);
        for ( int i = 0; i < messages.length; ++i )
        {
            messages[i].setMessageId(groupId);
            sendOneWay(messages[i], to[i]);
        }
        
        return iar;
    }
    
    public String sendRR(Message[][] messages, EndPoint[][] to, IAsyncCallback cb)
    {
        if ( messages.length != to.length )
        {
            throw new IllegalArgumentException("Number of messages and the number of endpoints need to be same.");
        }
        
        int length = messages.length;
        String[] gids = new String[length];
        /* Generate the requisite GUID's */
        for ( int i = 0; i < length; ++i )
        {
            gids[i] = GuidGenerator.guid();
        }
        /* attach this context to the callback */
        cb.attachContext(gids);
        for ( int i = 0; i < length; ++i )
        {
            callbackMap_.put(gids[i], cb);
            for ( int j = 0; j < messages[i].length; ++j )
            {
                messages[i][j].setMessageId(gids[i]);
                sendOneWay(messages[i][j], to[i][j]);
            }            
        }      
        return gids[0];
    }

    /*
        Use this version for fire and forget style messaging.
    */
    public void sendOneWay(Message message, EndPoint to)
    {        
        // do local deliveries        
        if ( message.getFrom().equals(to) )
        {            
            MessagingService.receive(message);
            return;
        }
        
        Runnable tcpWriteEvent = new MessageSerializationTask(message, to);
        messageSerializerExecutor_.execute(tcpWriteEvent);    
    }
    
    public IAsyncResult sendRR(Message message, EndPoint to)
    {
        IAsyncResult iar = new AsyncResult();
        taskCompletionMap_.put(message.getMessageId(), iar);
        sendOneWay(message, to);
        return iar;
    }
    
    public void sendUdpOneWay(Message message, EndPoint to)
    {
        EndPoint from = message.getFrom();              
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
    
    public void stream(String file, long startPosition, long total, EndPoint from, EndPoint to)
    {
        isStreaming_.set(true);
        /* Streaming asynchronously on streamExector_ threads. */
        Runnable streamingTask = new FileStreamTask(file, startPosition, total, from, to);
        streamExecutor_.execute(streamingTask);
    }
    
    /*
     * Does the application determine if we are currently streaming data.
     * This would imply either streaming to a receiver, receiving streamed
     * data or both. 
    */
    public static boolean isStreaming()
    {
        return isStreaming_.get();
    }
    
    public static void setStreamingMode(boolean bVal)
    {
        isStreaming_.set(bVal);
    }
    
    public static void shutdown()
    {
        logger_.info("Shutting down ...");
        synchronized ( MessagingService.class )
        {          
            /* Stop listening on any socket */            
            for( SelectionKey skey : listenSockets_.values() )
            {
                SelectorManager.getSelectorManager().cancel(skey);
            }
            listenSockets_.clear();
            
            /* Shutdown the threads in the EventQueue's */            
            messageDeserializationExecutor_.shutdownNow();            
            messageSerializerExecutor_.shutdownNow();
            messageDeserializerExecutor_.shutdownNow();
            streamExecutor_.shutdownNow();
            
            /* shut down the cachetables */
            taskCompletionMap_.shutdown();
            callbackMap_.shutdown();                        
                        
            /* Interrupt the selector manager thread */
            SelectorManager.getSelectorManager().interrupt();
            
            poolTable_.clear();            
            verbHandlers_.clear();                                    
            bShutdown_ = true;
        }
        logger_.debug("Shutdown invocation complete.");
    }

    public static void receive(Message message)
    {        
        enqueueRunnable(message.getMessageType(), new MessageDeliveryTask(message));
    }
    
    public static boolean isLocalEndPoint(EndPoint ep)
    {
        return ( endPoints_.contains(ep) );
    }
        
    private static void enqueueRunnable(String stageName, Runnable runnable){
        
        IStage stage = StageManager.getStage(stageName);   
        
        if ( stage != null )
        {
            logger_.info("Running on stage " + stage.getName());
            stage.execute(runnable);
        } 
        else
        {
            logger_.info("Running on default stage - beware");
            messageSerializerExecutor_.execute(runnable);
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
    
    public static void removeAsyncResult(String key)
    {
        taskCompletionMap_.remove(key);
    }

    public static byte[] getProtocol()
    {
        return protocol_;
    }
    
    public static ExecutorService getReadExecutor()
    {
        return messageDeserializationExecutor_;
    }
    
    public static ExecutorService getWriteExecutor()
    {
        return messageSerializerExecutor_;
    }
    
    public static ExecutorService getDeserilizationExecutor()
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
    
    public static ByteBuffer packIt(byte[] bytes, boolean compress, boolean stream, boolean listening)
    {
        byte[] size = toByteArray(bytes.length);
        /* 
             Setting up the protocol header. This is 4 bytes long
             represented as an integer. The first 2 bits indicate
             the serializer type. The 3rd bit indicates if compression
             is turned on or off. It is turned off by default. The 4th
             bit indicates if we are in streaming mode. It is turned off
             by default. The 5th bit is used to indicate that the sender
             is not listening on any well defined port. This implies the 
             receiver needs to cache the connection using the port on the 
             socket. The following 3 bits are reserved for future use. 
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
        
        // set listening 5th bit
        if ( listening )
            n |= 16;
        
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
