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

package org.apache.cassandra.service.epaxos;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class ForwardedQueryVerbHandler implements IVerbHandler<SerializedRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedQueryVerbHandler.class);

    private final EpaxosService service;

    public ForwardedQueryVerbHandler(EpaxosService service)
    {
        this.service = service;
    }

    @Override
    public void doVerb(final MessageIn<SerializedRequest> message, final int id)
    {
        final SerializedRequest request = message.payload;
        logger.debug("received forwarded query from {} for {}", message.from, request.getKey());
        Instance instance = service.createQueryInstance(request);
        SettableFuture future = service.setFuture(instance);

        Futures.addCallback(future, new FutureCallback()
        {
            @Override
            public void onSuccess(@Nullable Object result)
            {
                MessageOut<SerializedRequest.Result> msg = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                            request.wrapResult(result),
                                                                            SerializedRequest.Result.serializer);
                service.sendReply(msg, id, message.from);
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                logger.error("Error processing forwarded query {}", throwable);
            }
        });

        service.preaccept(instance);
    }
}
