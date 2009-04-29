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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.*;

import org.apache.cassandra.net.ProtocolHeader;
import org.apache.cassandra.net.TcpConnection;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TcpReader
{
    public static enum TcpReaderState
    {
        START,
        PREAMBLE,
        PROTOCOL,
        CONTENT_LENGTH,
        CONTENT,
        CONTENT_STREAM,
        DONE
    }
    
    private Map<TcpReaderState, StartState> stateMap_ = new HashMap<TcpReaderState, StartState>();
    private TcpConnection connection_;
    private StartState socketState_;
    private ProtocolHeader protocolHeader_;
    
    public TcpReader(TcpConnection connection)
    {
        connection_ = connection;        
    }
    
    public StartState getSocketState(TcpReaderState state)
    {
        return stateMap_.get(state);
    }
    
    public void putSocketState(TcpReaderState state, StartState socketState)
    {
        stateMap_.put(state, socketState);
    } 
    
    public void resetState()
    {
        StartState nextState = stateMap_.get(TcpReaderState.PREAMBLE);
        if ( nextState == null )
        {
            nextState = new ProtocolState(this);
            stateMap_.put(TcpReaderState.PREAMBLE, nextState);
        }
        socketState_ = nextState;
    }
    
    public void morphState(StartState state)
    {        
        socketState_ = state;
        if ( protocolHeader_ == null )
            protocolHeader_ = new ProtocolHeader();
    }
    
    public ProtocolHeader getProtocolHeader()
    {
        return protocolHeader_;
    }
    
    public SocketChannel getStream()
    {
        return connection_.getSocketChannel();
    }
    
    public byte[] read() throws IOException
    {
        byte[] bytes = new byte[0];      
        while ( socketState_ != null )
        {
            try
            {                                                                      
                bytes = socketState_.read();
            }
            catch ( ReadNotCompleteException e )
            {                
                break;
            }
        }
        return bytes;
    }    
    
    public static void main(String[] args) throws Throwable
    {
        Map<TcpReaderState, StartState> stateMap = new HashMap<TcpReaderState, StartState>();
        stateMap.put(TcpReaderState.CONTENT, new ContentState(null, 10));
        stateMap.put(TcpReaderState.START, new ProtocolState(null));
        stateMap.put(TcpReaderState.CONTENT_LENGTH, new ContentLengthState(null));
        
        StartState state = stateMap.get(TcpReaderState.CONTENT);
        System.out.println( state.getClass().getName() );
        state = stateMap.get(TcpReaderState.CONTENT_LENGTH);
        System.out.println( state.getClass().getName() );
    }
}
