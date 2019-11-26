/*
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

package org.apache.cassandra.streaming;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.net.ConnectionCategory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.StreamingSuccess;
import org.apache.cassandra.net.OutboundConnectionSettings;

import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateStreaming;

public class DefaultConnectionFactory implements StreamConnectionFactory
{
    @VisibleForTesting
    public static int MAX_CONNECT_ATTEMPTS = 3;

    @Override
    public Channel createConnection(OutboundConnectionSettings template, int messagingVersion) throws IOException
    {
        EventLoop eventLoop = MessagingService.instance().socketFactory.outboundStreamingGroup().next();

        int attempts = 0;
        while (true)
        {
            Future<Result<StreamingSuccess>> result = initiateStreaming(eventLoop, template.withDefaults(ConnectionCategory.STREAMING), messagingVersion);
            result.awaitUninterruptibly(); // initiate has its own timeout, so this is "guaranteed" to return relatively promptly
            if (result.isSuccess())
                return result.getNow().success().channel;

            if (++attempts == MAX_CONNECT_ATTEMPTS)
                throw new IOException("failed to connect to " + template.to + " for streaming data", result.cause());
        }
    }
}
