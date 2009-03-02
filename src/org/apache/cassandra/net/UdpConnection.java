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

import java.net.SocketAddress;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.net.io.ProtocolState;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.utils.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class UdpConnection extends SelectionKeyHandler
{
    private static Logger logger_ = Logger.getLogger(UdpConnection.class);
    private static final int BUFFER_SIZE = 4096;
    private static final int protocol_ = 0xBADBEEF;
    
    private DatagramChannel socketChannel_;
    private SelectionKey key_;    
    private EndPoint localEndPoint_;
    
    public void init() throws IOException
    {
        socketChannel_ = DatagramChannel.open();
        socketChannel_.socket().setReuseAddress(true);
        socketChannel_.configureBlocking(false);        
    }
    
    public void init(int port) throws IOException
    {
        // TODO: get TCP port from config and add one.
        localEndPoint_ = new EndPoint(port);
        socketChannel_ = DatagramChannel.open();
        socketChannel_.socket().bind(localEndPoint_.getInetAddress());
        socketChannel_.socket().setReuseAddress(true);
        socketChannel_.configureBlocking(false);        
        key_ = SelectorManager.getUdpSelectorManager().register(socketChannel_, this, SelectionKey.OP_READ);
    }
    
    public boolean write(Message message, EndPoint to) throws IOException
    {
        boolean bVal = true;                       
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        Message.serializer().serialize(message, dos);        
        byte[] data = bos.toByteArray();
        if ( data.length > 0 )
        {  
            logger_.debug("Size of Gossip packet " + data.length);
            byte[] protocol = BasicUtilities.intToByteArray(protocol_);
            ByteBuffer buffer = ByteBuffer.allocate(data.length + protocol.length);
            buffer.put( protocol );
            buffer.put(data);
            buffer.flip();
            
            int n  = socketChannel_.send(buffer, to.getInetAddress());
            if ( n == 0 )
            {
                bVal = false;
            }
        }
        return bVal;
    }
    
    void close()
    {
        try
        {
            if ( socketChannel_ != null )
                socketChannel_.close();
        }
        catch ( IOException ex )
        {
            logger_.error( LogUtil.throwableToString(ex) );
        }
    }
    
    public DatagramChannel getDatagramChannel()
    {
        return socketChannel_;
    }
    
    private byte[] gobbleHeaderAndExtractBody(ByteBuffer buffer)
    {
        byte[] body = new byte[0];        
        byte[] protocol = new byte[4];
        buffer = buffer.get(protocol, 0, protocol.length);
        int value = BasicUtilities.byteArrayToInt(protocol);
        
        if ( protocol_ != value )
        {
            logger_.info("Invalid protocol header in the incoming message " + value);
            return body;
        }
        body = new byte[buffer.remaining()];
        buffer.get(body, 0, body.length);       
        return body;
    }
    
    public void read(SelectionKey key)
    {        
        key.interestOps( key.interestOps() & (~SelectionKey.OP_READ) );
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        try
        {
            SocketAddress sa = socketChannel_.receive(buffer);
            if ( sa == null )
            {
                logger_.debug("*** No datagram packet was available to be read ***");
                return;
            }            
            buffer.flip();
            
            byte[] bytes = gobbleHeaderAndExtractBody(buffer);
            if ( bytes.length > 0 )
            {
                DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );
                Message message = Message.serializer().deserialize(dis);                
                if ( message != null )
                {                                        
                    MessagingService.receive(message);
                }
            }
        }
        catch ( IOException ioe )
        {
            logger_.warn(LogUtil.throwableToString(ioe));
        }
        finally
        {
            key.interestOps( key_.interestOps() | SelectionKey.OP_READ );
        }
    }
}
