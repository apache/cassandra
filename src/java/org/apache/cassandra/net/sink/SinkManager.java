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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.net.Message;

public class SinkManager
{
    private static List<IMessageSink> sinks = new ArrayList<IMessageSink>();

    public static void add(IMessageSink ms)
    {
        sinks.add(ms);
    }

    public static void clear()
    {
        sinks.clear();
    }

    public static Message processClientMessage(Message message, String id, InetAddress to)
    {
        if (sinks.isEmpty())
            return message;

        for (IMessageSink ms : sinks)
        {
            message = ms.handleMessage(message, id, to);
            if (message == null)
                return null;
        }
        return message;
    }

    public static Message processServerMessage(Message message, String id)
    {
        if (sinks.isEmpty())
            return message;

        for (IMessageSink ms : sinks)
        {
            message = ms.handleMessage(message, id, null);
            if (message == null)
                return null;
        }
        return message;
    }
}
