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


import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.UpgradeService;

public class PrepareVerbHandler implements IVerbHandler<Commit>
{
    private static MessageOut<PrepareResponse> failureMessage(Commit commit)
    {
        PrepareResponse response = new PrepareResponse(false, commit, commit);
        MessageOut<PrepareResponse> message = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, PrepareResponse.serializer);
        message = message.withParameter(UpgradeService.PAXOS_UPGRADE_ERROR, new byte[]{});
        return message;
    }


    public void doVerb(MessageIn<Commit> message, int id)
    {
        if (UpgradeService.instance().isUpgradedForQuery(message))
        {
            MessagingService.instance().sendReply(failureMessage(message.payload), id, message.from);
        }
        else
        {
            PrepareResponse response = PaxosState.prepare(message.payload);
            MessageOut<PrepareResponse> reply = new MessageOut<PrepareResponse>(MessagingService.Verb.REQUEST_RESPONSE, response, PrepareResponse.serializer);
            MessagingService.instance().sendReply(reply, id, message.from);
        }
    }
}
