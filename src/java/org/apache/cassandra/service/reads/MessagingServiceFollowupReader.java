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

package org.apache.cassandra.service.reads;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;

/**
 * Messaging service direct implementation of follow up reads that bypasses
 * any transaction system and performs unprotected/inconsistent reads
 */
public class MessagingServiceFollowupReader implements CassandraFollowupReader
{
    public static final MessagingServiceFollowupReader instance = new MessagingServiceFollowupReader();

    private MessagingServiceFollowupReader() {}

    public void read(ReadCommand command, Replica replica, ReadCallback callback, boolean trackRepairedStatus)
    {
        if (replica.isSelf())
        {
            Stage.READ.maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(command, callback, trackRepairedStatus));
        }
        else
        {
            if (replica.isTransient())
                command = command.copyAsTransientQuery(replica);
            Message<ReadCommand> message = command.createMessage(trackRepairedStatus && replica.isFull());
            MessagingService.instance().sendWithCallback(message, replica.endpoint(), callback);
        }
    }
}
