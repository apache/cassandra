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
package org.apache.cassandra.db;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.EpaxosService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

public class ReadRepairVerbHandler implements IVerbHandler<Mutation>
{
    private static final Logger logger = LoggerFactory.getLogger(ReadRepairVerbHandler.class);

    private final EpaxosService state;

    public ReadRepairVerbHandler()
    {
        this(EpaxosService.getInstance());
    }

    public ReadRepairVerbHandler(EpaxosService state)
    {
        this.state = state;
    }

    protected void sendResponse(int id, InetAddress from)
    {
        WriteResponse response = new WriteResponse();
        MessagingService.instance().sendReply(response.createMessage(), id, from);
    }

    protected void applyMutation(Mutation mutation)
    {
        mutation.apply();
    }

    @Override
    public void doVerb(MessageIn<Mutation> message, int id)
    {
        Mutation mutation = message.payload;
        assert mutation.getColumnFamilyIds().size() == 1;
        UUID cfId = mutation.getColumnFamilyIds().iterator().next();
        if (state.shouldApplyRepair(mutation.key(), cfId, message))
        {
            logger.debug("Applying read repair");
            applyMutation(mutation);
        }
        else
        {
            logger.debug("Skipping read repair message with data from unexecuted epaxos instance");
        }
        sendResponse(id, message.from);
    }
}
