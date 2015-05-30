package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.Scope;
import org.apache.cassandra.service.epaxos.UpgradeService;
import org.apache.cassandra.utils.BooleanSerializer;

import static org.apache.cassandra.service.epaxos.UpgradeService.*;

public class ProposeVerbHandler implements IVerbHandler<Commit>
{

    private static final MessageOut<Boolean> UPGRADE_FAILURE;
    static
    {
        MessageOut<Boolean> msg = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, false, BooleanSerializer.serializer);
        UPGRADE_FAILURE = msg.withParameter(UpgradeService.PAXOS_UPGRADE_ERROR, new byte[]{});
    }

    public void doVerb(MessageIn<Commit> message, int id)
    {
        if (UpgradeService.instance().isUpgradedForQuery(message))
        {
            MessagingService.instance().sendReply(UPGRADE_FAILURE, id, message.from);
        }
        else
        {
            Boolean response = PaxosState.propose(message.payload);
            Set<UUID> deps = null;
            if (response)
            {
                ConsistencyLevel cl = clFromBytes(message.parameters.get(PAXOS_CONSISTEMCY_PARAM));
                deps = UpgradeService.instance().reportPaxosProposal(message.payload, message.from, cl);
            }

            MessageOut<Boolean> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, BooleanSerializer.serializer);

            if (deps != null)
            {
                reply = reply.withParameter(PAXOS_DEPS_PARAM, depsToBytes(deps));
            }

            MessagingService.instance().sendReply(reply, id, message.from);
        }
    }
}
