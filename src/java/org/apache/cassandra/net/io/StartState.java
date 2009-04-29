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

import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.io.IOException;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public abstract class StartState
{
    protected TcpReader stream_;

    public StartState(TcpReader stream)
    {
        stream_ = stream;
    }

    public abstract byte[] read() throws IOException, ReadNotCompleteException;
    public abstract void morphState() throws IOException;
    public abstract void setContextData(Object data);

    protected byte[] doRead(ByteBuffer buffer) throws IOException, ReadNotCompleteException
    {        
        SocketChannel socketChannel = stream_.getStream();
        int bytesRead = socketChannel.read(buffer);     
        if ( bytesRead == -1 && buffer.remaining() > 0 )
        {            
            throw new IOException("Reached an EOL or something bizzare occured. Reading from: " + socketChannel.socket().getInetAddress() + " BufferSizeRemaining: " + buffer.remaining());
        }
        if ( buffer.remaining() == 0 )
        {
            morphState();
        }
        else
        {            
            throw new ReadNotCompleteException("Specified number of bytes have not been read from the Socket Channel");
        }
        return new byte[0];
    }
}
