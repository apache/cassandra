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

package org.apache.cassandra.net.sink;

import java.util.*;
import java.io.IOException;

import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SinkManager
{
    private static LinkedList<IMessageSink> messageSinks_ = new LinkedList<IMessageSink>();

    public static boolean isInitialized()
    {
        return ( messageSinks_.size() > 0 );
    }

    public static void addMessageSink(IMessageSink ms)
    {
        messageSinks_.addLast(ms);
    }
    
    public static void clearSinks(){
        messageSinks_.clear();
    }

    public static Message processClientMessageSink(Message message)
    {
        ListIterator<IMessageSink> li = messageSinks_.listIterator();
        while ( li.hasNext() )
        {
            IMessageSink ms = li.next();
            message = ms.handleMessage(message);
            if ( message == null )
            {
                return null;
            }
        }
        return message;
    }

    public static Message processServerMessageSink(Message message)
    {
        ListIterator<IMessageSink> li = messageSinks_.listIterator(messageSinks_.size());
        while ( li.hasPrevious() )
        {
            IMessageSink ms = li.previous();
            message = ms.handleMessage(message);
            if ( message == null )
            {
                return null;
            }
        }
        return message;
    }
}
