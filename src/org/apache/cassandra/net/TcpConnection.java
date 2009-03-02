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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.io.FastSerializer;
import org.apache.cassandra.net.io.ISerializer;
import org.apache.cassandra.net.io.ProtocolState;
import org.apache.cassandra.net.io.StartState;
import org.apache.cassandra.net.io.TcpReader;
import org.apache.cassandra.net.io.TcpReader.TcpReaderState;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.net.io.*;
import org.apache.cassandra.net.sink.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TcpConnection extends SelectionKeyHandler implements Comparable
{
    // logging and profiling.
    private static Logger logger_ = Logger.getLogger(TcpConnection.class);  
    private static ISerializer serializer_ = new FastSerializer();
    private SocketChannel socketChannel_;
    private SelectionKey key_;
    private TcpConnectionManager pool_;
    private boolean isIncoming_ = false;       
    private TcpReader tcpReader_;    
    private ReadWorkItem readWork_ = new ReadWorkItem(); 
    private List<ByteBuffer> pendingWrites_ = new Vector<ByteBuffer>();  
    private AtomicBoolean connected_ = new AtomicBoolean(false);    
    private EndPoint localEp_;
    private EndPoint remoteEp_;
    boolean inUse_ = false;
         
    /* 
     * Added for streaming support. We need the boolean
     * to indicate that this connection is used for 
     * streaming. The Condition and the Lock are used
     * to signal the stream() that it can continue
     * streaming when the socket becomes writable.
    */
    private boolean bStream_ = false;
    private Lock lock_;
    private Condition condition_;
    
    // used from getConnection - outgoing
    TcpConnection(TcpConnectionManager pool, EndPoint from, EndPoint to) throws IOException
    {          
        socketChannel_ = SocketChannel.open();            
        socketChannel_.configureBlocking(false);        
        pool_ = pool;
        
        localEp_ = from;
        remoteEp_ = to;
        
        if ( !socketChannel_.connect( remoteEp_.getInetAddress() ) )
        {
            key_ = SelectorManager.getSelectorManager().register(socketChannel_, this, SelectionKey.OP_CONNECT);
        }
        else
        {
            key_ = SelectorManager.getSelectorManager().register(socketChannel_, this, SelectionKey.OP_READ);
            connected_.set(true);     
        }         
    }
    
    /*
     * Used for streaming purposes has no pooling semantics.
    */
    TcpConnection(EndPoint from, EndPoint to) throws IOException
    {
        socketChannel_ = SocketChannel.open();               
        socketChannel_.configureBlocking(false);       
        
        localEp_ = from;
        remoteEp_ = to;
        
        if ( !socketChannel_.connect( remoteEp_.getInetAddress() ) )
        {
            key_ = SelectorManager.getSelectorManager().register(socketChannel_, this, SelectionKey.OP_CONNECT);
        }
        else
        {
            key_ = SelectorManager.getSelectorManager().register(socketChannel_, this, SelectionKey.OP_READ);
            connected_.set(true);     
        }        
        bStream_ = true;
        lock_ = new ReentrantLock();
        condition_ = lock_.newCondition();
    }
    
    /*
     * This method is invoked by the TcpConnectionHandler to accept incoming TCP connections.
     * Accept the connection and then register interest for reads.
    */
    static void acceptConnection(SocketChannel socketChannel, EndPoint localEp, boolean isIncoming) throws IOException
    {
        TcpConnection tcpConnection = new TcpConnection(socketChannel, localEp, true);
        tcpConnection.registerReadInterest();
    }
    
    private void registerReadInterest() throws IOException
    {
        key_ = SelectorManager.getSelectorManager().register(socketChannel_, this, SelectionKey.OP_READ);
    }
    
    // used for incoming connections
    TcpConnection(SocketChannel socketChannel, EndPoint localEp, boolean isIncoming) throws IOException
    {       
        socketChannel_ = socketChannel;
        socketChannel_.configureBlocking(false);                           
        isIncoming_ = isIncoming;
        connected_.set(true);       
        localEp_ = localEp;           
    }
    
    EndPoint getLocalEp()
    {
        return localEp_;
    }
    
    public void setLocalEp(EndPoint localEp)
    {
        localEp_ = localEp;
    }

    public EndPoint getEndPoint() 
    {
        return remoteEp_;
    }
    
    public boolean isIncoming()
    {
        return isIncoming_;
    }    
    
    public SocketChannel getSocketChannel()
    {
        return socketChannel_;
    }    
    
    public void write(Message message) throws IOException
    {           
        byte[] data = serializer_.serialize(message);        
        if ( data.length > 0 )
        {    
            boolean listening = ( message.getFrom().equals(EndPoint.randomLocalEndPoint_) ) ? false : true;
            ByteBuffer buffer = MessagingService.packIt( data , false, false, listening);   
            synchronized(this)
            {
                if (!pendingWrites_.isEmpty() || !connected_.get()) 
                {                     
                    pendingWrites_.add(buffer);                
                    return;
                }
                
                logger_.debug("Sending packets of size " + data.length);            
                socketChannel_.write(buffer);                
                
                if (buffer.remaining() > 0) 
                {                   
                    pendingWrites_.add(buffer);
                    if ((key_.interestOps() & SelectionKey.OP_WRITE) == 0)
                    {                                    
                        SelectorManager.getSelectorManager().modifyKeyForWrite(key_);                     
                    }
                }
            }
        }
    }
    
    public void stream(File file, long startPosition, long endPosition) throws IOException
    {
        if ( !bStream_ )
            throw new IllegalStateException("Cannot stream since we are not set up to stream data.");
                
        lock_.lock();        
        try
        {            
            /* transfer 64MB in each attempt */
            int limit = 64*1024*1024;  
            long total = endPosition - startPosition;
            /* keeps track of total number of bytes transferred */
            long bytesWritten = 0L;                          
            RandomAccessFile raf = new RandomAccessFile(file, "r");            
            FileChannel fc = raf.getChannel();            
            
            /* 
             * If the connection is not yet established then wait for
             * the timeout period of 2 seconds. Attempt to reconnect 3 times and then 
             * bail with an IOException.
            */
            long waitTime = 2;
            int retry = 0;
            while ( !connected_.get() )
            {
                if ( retry == 3 )
                    throw new IOException("Unable to connect to " + remoteEp_ + " after " + retry + " attempts.");
                waitToContinueStreaming(waitTime, TimeUnit.SECONDS);
                ++retry;
            }
            
            while ( bytesWritten < total )
            {                                
                if ( startPosition == 0 )
                {
                    ByteBuffer buffer = MessagingService.constructStreamHeader(false, true);                      
                    socketChannel_.write(buffer);
                    handleIncompleteWrite(buffer);
                }
                
                /* returns the number of bytes transferred from file to the socket */
                long bytesTransferred = fc.transferTo(startPosition, limit, socketChannel_);
                logger_.debug("Bytes transferred " + bytesTransferred);                
                bytesWritten += bytesTransferred;
                startPosition += bytesTransferred; 
                /*
                 * If the number of bytes transferred is less than intended 
                 * then we need to wait till socket becomes writeable again. 
                */
                if ( bytesTransferred < limit && bytesWritten != total )
                {                    
                    if ((key_.interestOps() & SelectionKey.OP_WRITE) == 0)
                    {                    
                        SelectorManager.getSelectorManager().modifyKeyForWrite(key_);                     
                    }
                    waitToContinueStreaming();
                }
            }
        }
        finally
        {
            lock_.unlock();
        }        
    }
    
    private void handleIncompleteWrite(ByteBuffer buffer)
    {
        if (buffer.remaining() > 0) 
        {            
            pendingWrites_.add(buffer);
            if ((key_.interestOps() & SelectionKey.OP_WRITE) == 0)
            {                    
                SelectorManager.getSelectorManager().modifyKeyForWrite(key_);                     
            }
            waitToContinueStreaming();
        }     
    }
    
    private void waitToContinueStreaming()
    {
        try
        {
            condition_.await();
        }
        catch ( InterruptedException ex )
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
    }
    
    private void waitToContinueStreaming(long waitTime, TimeUnit tu)
    {
        try
        {
            condition_.await(waitTime, tu);
        }
        catch ( InterruptedException ex )
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
    }
    
    private void resumeStreaming()
    {
        /* if not in streaming mode do nothing */
        if ( !bStream_ )
            return;
        
        lock_.lock();
        try
        {
            condition_.signal();
        }
        finally
        {
            lock_.unlock();
        }
    }
    
    public void close()
    {
        inUse_ = false;
        if ( pool_.contains(this) )
            pool_.decUsed();               
    }

    public boolean isConnected()
    {
        return socketChannel_.isConnected();
    }
    
    public boolean equals(Object o)
    {
        if ( !(o instanceof TcpConnection) )
            return false;
        
        TcpConnection rhs = (TcpConnection)o;        
        if ( localEp_.equals(rhs.localEp_) && remoteEp_.equals(rhs.remoteEp_) )
            return true;
        else
            return false;
    }
    
    public int hashCode()
    {
        return (localEp_ + ":" + remoteEp_).hashCode();
    }

    public String toString()
    {        
        return socketChannel_.toString();
    }
    
    void closeSocket()
    {
        logger_.warn("Closing down connection " + socketChannel_ + " with " + pendingWrites_.size() + " writes remaining.");            
        if ( pool_ != null )
        {
            pool_.removeConnection(this);
        }
        cancel(key_);
        pendingWrites_.clear();
    }
    
    void errorClose() 
    {        
        logger_.warn("Closing down connection " + socketChannel_);
        pendingWrites_.clear();
        cancel(key_);
        pendingWrites_.clear();        
        if ( pool_ != null )
        {
            pool_.removeConnection(this);            
        }
    }
    
    private void cancel(SelectionKey key)
    {
        if ( key != null )
            SelectorManager.getSelectorManager().cancel(key);
    }
    
    // called in the selector thread
    public void connect(SelectionKey key)
    {       
        key.interestOps(key.interestOps() & (~SelectionKey.OP_CONNECT));        
        try
        {
            if (socketChannel_.finishConnect())
            {                                
                SelectorManager.getSelectorManager().modifyKeyForRead(key);
                connected_.set(true);                
                
                // this will flush the pending                
                if (!pendingWrites_.isEmpty()) 
                {                    
                    SelectorManager.getSelectorManager().modifyKeyForWrite(key_);  
                } 
                resumeStreaming();
            } 
            else 
            {  
                logger_.warn("Closing connection because socket channel could not finishConnect.");;
                errorClose();
            }
        } 
        catch(IOException e) 
        {               
            logger_.warn("Encountered IOException on connection: "  + socketChannel_);
            logger_.warn( LogUtil.throwableToString(e) );
            errorClose();
        }
    }
    
    // called in the selector thread
    public void write(SelectionKey key)
    {   
        key.interestOps( key.interestOps() & ( ~SelectionKey.OP_WRITE ) );                
        doPendingWrites();
        /*
         * This is executed only if we are in streaming mode.
         * Idea is that we read a chunk of data from a source
         * and wait to read the next from the source until we 
         * are siganlled to do so from here. 
        */
         resumeStreaming();        
    }
    
    void doPendingWrites()
    {
        try
        {                     
            while(!pendingWrites_.isEmpty()) 
            {
                ByteBuffer buffer = pendingWrites_.get(0);
                socketChannel_.write(buffer);                    
                if (buffer.remaining() > 0) 
                {   
                    break;
                }               
                pendingWrites_.remove(0);                    
            } 
            
        }
        catch(IOException ex)
        {
            logger_.warn(LogUtil.throwableToString(ex));
            // This is to fix the wierd Linux bug with NIO.
            errorClose();
        }
        finally
        {    
            synchronized(this)
            {
                if (!pendingWrites_.isEmpty() && (key_.interestOps() & SelectionKey.OP_WRITE) == 0)
                {                    
                    SelectorManager.getSelectorManager().modifyKeyForWrite(key_); 
                }  
            }
        }
    }
    
    // called in the selector thread
    public void read(SelectionKey key)
    {          
        key.interestOps( key.interestOps() & ( ~SelectionKey.OP_READ ) );         
        // publish this event onto to the TCPReadEvent Queue.
        MessagingService.getReadExecutor().execute(readWork_);
    }
    
    public void modifyKeyForRead(SelectionKey key)
    {
        key.interestOps( key_.interestOps() | SelectionKey.OP_READ );
    }
    
    public void modifyKeyForWrite(SelectionKey key)
    {        
        key.interestOps( key_.interestOps() | SelectionKey.OP_WRITE );
    }
    
    class ReadWorkItem implements Runnable
    {                 
        // called from the TCP READ thread pool
        public void run()
        {                         
            if ( tcpReader_ == null )
            {
                tcpReader_ = new TcpReader(TcpConnection.this);    
                StartState nextState = tcpReader_.getSocketState(TcpReader.TcpReaderState.PREAMBLE);
                if ( nextState == null )
                {
                    nextState = new ProtocolState(tcpReader_);
                    tcpReader_.putSocketState(TcpReader.TcpReaderState.PREAMBLE, nextState);
                }
                tcpReader_.morphState(nextState);
            }
            
            try
            {           
                byte[] bytes = new byte[0];
                while ( (bytes = tcpReader_.read()).length > 0 )
                {                       
                    ProtocolHeader pH = tcpReader_.getProtocolHeader();                    
                    if ( !pH.isStreamingMode_ )
                    {
                        /* first message received */
                        if (remoteEp_ == null)
                        {             
                            int port = ( pH.isListening_ ) ? DatabaseDescriptor.getStoragePort() : EndPoint.randomPort_;
                            remoteEp_ = new EndPoint( socketChannel_.socket().getInetAddress().getHostName(), port );                            
                            // put connection into pool if possible
                            pool_ = MessagingService.getConnectionPool(localEp_, remoteEp_);                            
                            pool_.addToPool(TcpConnection.this);                            
                        }
                        
                        /* Deserialize and handle the message */
                        MessagingService.getDeserilizationExecutor().submit( new MessageDeserializationTask(pH.serializerType_, bytes) );                                                  
                        tcpReader_.resetState();
                    }
                    else
                    {
                        MessagingService.setStreamingMode(false);
                        /* Close this socket connection  used for streaming */
                        closeSocket();
                    }                    
                }
            }
            catch ( IOException ex )
            {                   
                handleException(ex);
            }
            catch ( Throwable th )
            {
                handleException(th);
            }
            finally
            {                                     
                SelectorManager.getSelectorManager().modifyKeyForRead(key_);                
            }
        }
        
        private void handleException(Throwable th)
        {
            logger_.warn("Problem reading from socket connected to : " + socketChannel_);
            logger_.warn(LogUtil.throwableToString(th));
            // This is to fix the wierd Linux bug with NIO.
            errorClose();
        }
    }
    
    public int pending()
    {
        return pendingWrites_.size();
    }
    
    public int compareTo(Object o)
    {
        if (o instanceof TcpConnection) 
        {
            return pendingWrites_.size() - ((TcpConnection) o).pendingWrites_.size();            
        }
                    
        throw new IllegalArgumentException();
    }
}
