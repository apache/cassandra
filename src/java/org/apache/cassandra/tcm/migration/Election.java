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

package org.apache.cassandra.tcm.migration;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class Election
{
    private static final Logger logger = LoggerFactory.getLogger(Election.class);

    public static <REQ, RSP> Collection<Pair<InetAddressAndPort, RSP>> fanoutAndWait(MessageDelivery messaging, Set<InetAddressAndPort> sendTo, Verb verb, REQ payload)
    {
        Accumulator<Pair<InetAddressAndPort, RSP>> responses = new Accumulator<>(sendTo.size());
        CountDownLatch cdl = CountDownLatch.newCountDownLatch(sendTo.size());
        RequestCallback<RSP> callback = new RequestCallbackWithFailure<RSP>()
        {
            @Override
            public void onResponse(Message<RSP> msg)
            {
                logger.debug("Received a {} response from {}: {}", msg.verb(), msg.from(), msg.payload);
                responses.add(Pair.create(msg.from(), msg.payload));
                cdl.decrement();
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
            {
                logger.debug("Received a failure response from {}: {}", from, reason);
                cdl.decrement();
            }
        };

        sendTo.forEach((ep) -> messaging.sendWithCallback(Message.out(verb, payload), ep, callback));
        cdl.awaitUninterruptibly(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        return responses.snapshot();
    }
}
