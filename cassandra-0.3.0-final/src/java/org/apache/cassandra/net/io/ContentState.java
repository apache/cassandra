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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.io.IOException;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class ContentState extends StartState
{
    private ByteBuffer buffer_;   
    private int length_;

    ContentState(TcpReader stream, int length)
    {
        super(stream);
        length_ = length; 
        buffer_ = ByteBuffer.allocate(length_);
    }

    public byte[] read() throws IOException, ReadNotCompleteException
    {          
        return doRead(buffer_);
    }

    public void morphState() throws IOException
    {        
        StartState nextState = stream_.getSocketState(TcpReader.TcpReaderState.DONE);
        if ( nextState == null )
        {
            nextState = new DoneState(stream_, toBytes());
            stream_.putSocketState( TcpReader.TcpReaderState.DONE, nextState );
        }
        else
        {            
            nextState.setContextData(toBytes());
        }
        stream_.morphState( nextState );               
    }
    
    private byte[] toBytes()
    {
        buffer_.position(0); 
        /*
        ByteBuffer slice = buffer_.slice();        
        return slice.array();
        */  
        byte[] bytes = new byte[length_];
        buffer_.get(bytes, 0, length_);
        return bytes;
    }
    
    public void setContextData(Object data)
    {
        Integer value = (Integer)data;
        length_ = value;               
        buffer_.clear();
        if ( buffer_.capacity() < length_ )
            buffer_ = ByteBuffer.allocate(length_);
        else
        {            
            buffer_.limit(length_);
        }        
    }
}
