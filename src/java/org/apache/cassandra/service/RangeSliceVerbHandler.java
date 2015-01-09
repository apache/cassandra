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
package org.apache.cassandra.service;

import org.apache.cassandra.db.AbstractRangeCommand;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

public class RangeSliceVerbHandler implements IVerbHandler<AbstractRangeCommand>
{
    public void doVerb(MessageIn<AbstractRangeCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            /* Don't service reads! */
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }
        RangeSliceReply reply = new RangeSliceReply(message.payload.executeLocally());
        Tracing.trace("Enqueuing response to {}", message.from);
        MessagingService.instance().sendReply(reply.createMessage(), id, message.from);
    }
}
