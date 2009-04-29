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

package org.apache.cassandra.net.io;

import org.apache.cassandra.utils.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.io.IOException;
import org.apache.cassandra.net.MessagingService;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ProtocolState extends StartState
{
    private ByteBuffer buffer_;

    public ProtocolState(TcpReader stream)
    {
        super(stream);
        buffer_ = ByteBuffer.allocate(16);
    }

    public byte[] read() throws IOException, ReadNotCompleteException
    {        
        return doRead(buffer_);
    }

    public void morphState() throws IOException
    {
        byte[] protocol = buffer_.array();
        if ( MessagingService.isProtocolValid(protocol) )
        {            
            StartState nextState = stream_.getSocketState(TcpReader.TcpReaderState.PROTOCOL);
            if ( nextState == null )
            {
                nextState = new ProtocolHeaderState(stream_);
                stream_.putSocketState( TcpReader.TcpReaderState.PROTOCOL, nextState );
            }
            stream_.morphState( nextState ); 
            buffer_.clear();
        }
        else
        {
            throw new IOException("Invalid protocol header. The preamble seems to be messed up.");
        }
    }
    
    public void setContextData(Object data)
    {
        throw new UnsupportedOperationException("This method is not supported in the ProtocolState");
    }
}

