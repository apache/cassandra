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

import org.apache.cassandra.utils.FBUtilities;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class ContentLengthState extends StartState
{
    private ByteBuffer buffer_;

    ContentLengthState(TcpReader stream)
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
        int size = FBUtilities.byteArrayToInt(buffer_.array());        
        StartState nextState = stream_.getSocketState(TcpReader.TcpReaderState.CONTENT);
        if ( nextState == null )
        {
            nextState = new ContentState(stream_, size);
            stream_.putSocketState( TcpReader.TcpReaderState.CONTENT, nextState );
        }
        else
        {               
            nextState.setContextData(size);
        }
        stream_.morphState( nextState );
        buffer_.clear();
    }
    
    public void setContextData(Object data)
    {
        throw new UnsupportedOperationException("This method is not supported in the ContentLengthState");
    }
}
