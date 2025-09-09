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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.messages.IncomingMutationLogStreamMessage;
import org.apache.cassandra.streaming.messages.MutationLogReceivedMessage;

/**
 * Task for tracking reception of mutation log streams.
 */
public class LogReceiveTask extends LogStreamTask
{

    public LogReceiveTask(StreamSession session, InetAddressAndPort peer)
    {
        super(session, peer);
    }

    public synchronized void received(IncomingMutationLogStreamMessage message)
    {
        // TODO: validate message header with expected ranges
        if (markCompleted())
        {
            session.taskCompleted(this);
            // Send acknowledgment on successful completion
            session.sendControlMessage(new MutationLogReceivedMessage()).syncUninterruptibly();

        }
    }

    @Override
    public void abort()
    {
        // cleanup if needed
    }
}