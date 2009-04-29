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

package org.apache.cassandra.net.http;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.SelectionKeyHandler;
import org.apache.cassandra.net.SelectorManager;
import org.apache.log4j.Logger;

public class HttpConnectionHandler extends SelectionKeyHandler
{
    private static Logger logger_ = Logger.getLogger(HttpConnectionHandler.class);
    
    public void accept(SelectionKey key)
    {
        try
        {
            ServerSocketChannel serverChannel = (ServerSocketChannel)key.channel();
            SocketChannel client = serverChannel.accept();
            if ( client != null )
            {
                client.configureBlocking(false);            
                SelectionKeyHandler handler = new HttpConnection();
                SelectorManager.getSelectorManager().register(client, handler, SelectionKey.OP_READ);
            }
        } 
        catch(IOException e) 
        {
            logger_.warn(e);
        }
    }
}
