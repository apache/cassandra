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
package org.apache.cassandra.schema;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;

/**
 * Sends it's current schema state in form of mutations in response to the remote node's request.
 * Such a request is made when one of the nodes, by means of Gossip, detects schema disagreement in the ring.
 */
public final class SchemaPullVerbHandler implements IVerbHandler<NoPayload>
{
    public static final SchemaPullVerbHandler instance = new SchemaPullVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(SchemaPullVerbHandler.class);

    private final List<Consumer<Message<NoPayload>>> handlers = new CopyOnWriteArrayList<>();

    public void register(Consumer<Message<NoPayload>> handler)
    {
        handlers.add(handler);
    }

    public void doVerb(Message<NoPayload> message)
    {
        logger.trace("Received schema pull request from {}", message.from());
        List<Consumer<Message<NoPayload>>> handlers = this.handlers;
        if (handlers.isEmpty())
            throw new UnsupportedOperationException("There is no handler registered for schema pull verb");

        handlers.forEach(h -> h.accept(message));
    }
}
