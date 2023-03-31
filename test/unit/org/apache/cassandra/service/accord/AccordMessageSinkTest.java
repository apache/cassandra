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

package org.apache.cassandra.service.accord;

import org.apache.cassandra.net.MessageDelivery;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.messages.InformOfTxnId;
import accord.messages.SimpleReply;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.mockito.Mockito;

public class AccordMessageSinkTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void informOfTxn()
    {
        // There was an issue where the reply was the wrong verb
        // see CASSANDRA-18375
        InformOfTxnId info = Mockito.mock(InformOfTxnId.class);
        Message<InformOfTxnId> req = Message.builder(Verb.ACCORD_INFORM_OF_TXNID_REQ, info).build();
        SimpleReply reply = SimpleReply.Ok;

        MessageDelivery messaging = Mockito.mock(MessageDelivery.class);
        AccordMessageSink sink = new AccordMessageSink(messaging);
        sink.reply(new Node.Id(1), req, reply);

        Mockito.verify(messaging).send(Mockito.any(), Mockito.any());
    }
}