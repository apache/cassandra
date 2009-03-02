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

public class ProtocolHeaderState extends StartState
{
    private ByteBuffer buffer_;

    public ProtocolHeaderState(TcpReader stream)
    {
        super(stream);
        buffer_ = ByteBuffer.allocate(4);
    }

    public byte[] read() throws IOException, ReadNotCompleteException
    {        
        return doRead(buffer_);
    }

    public void morphState() throws IOException
    {
        byte[] protocolHeader = buffer_.array();
        int pH = MessagingService.byteArrayToInt(protocolHeader);
        
        int type = MessagingService.getBits(pH, 1, 2);
        stream_.getProtocolHeader().serializerType_ = type;
        
        int stream = MessagingService.getBits(pH, 3, 1);
        stream_.getProtocolHeader().isStreamingMode_ = (stream == 1) ? true : false;
        
        if ( stream_.getProtocolHeader().isStreamingMode_ )
            MessagingService.setStreamingMode(true);
        
        int listening = MessagingService.getBits(pH, 4, 1);
        stream_.getProtocolHeader().isListening_ = (listening == 1) ? true : false;
        
        int version = MessagingService.getBits(pH, 15, 8);
        stream_.getProtocolHeader().version_ = version;
        
        if ( version <= MessagingService.getVersion() )
        {
            if ( stream_.getProtocolHeader().isStreamingMode_ )
            { 
                StartState nextState = stream_.getSocketState(TcpReader.TcpReaderState.CONTENT_STREAM);
                if ( nextState == null )
                {
                    nextState = new ContentStreamState(stream_);
                    stream_.putSocketState( TcpReader.TcpReaderState.CONTENT_STREAM, nextState );
                }
                stream_.morphState( nextState );
                buffer_.clear();
            }
            else
            {
                StartState nextState = stream_.getSocketState(TcpReader.TcpReaderState.CONTENT_LENGTH);
                if ( nextState == null )
                {
                    nextState = new ContentLengthState(stream_);
                    stream_.putSocketState( TcpReader.TcpReaderState.CONTENT_LENGTH, nextState );
                }                
                stream_.morphState( nextState );   
                buffer_.clear();
            }            
        }
        else
        {
            throw new IOException("Invalid version in message. Scram.");
        }
    }
    
    public void setContextData(Object data)
    {
        throw new UnsupportedOperationException("This method is not supported in the ProtocolHeaderState");
    }
}


