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
package org.apache.cassandra.service.accord.async;

import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.PreLoadContext;
import org.apache.cassandra.service.accord.AccordCommandStore;

/**
 * Durably appends PreAccept, Accept, Commit, and Apply messages to {@code AccordJournal}
 * before any further steps can be attempted.
 */
public class AsyncAppender implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncAppender.class);

    enum State { INITIALIZED, WAITING, FINISHED }
    private State state = State.INITIALIZED;

    private final AccordCommandStore commandStore;
    private final PreLoadContext preLoadContext;
    private final BiConsumer<Object, Throwable> callback;

    public AsyncAppender(AccordCommandStore commandStore, PreLoadContext preLoadContext, BiConsumer<Object, Throwable> callback)
    {
        this.commandStore = commandStore;
        this.preLoadContext = preLoadContext;
        this.callback = callback;
    }

    public boolean append()
    {
        if (!commandStore.mustAppendToJournal(preLoadContext))
        {
            logger.trace("Skipping append for {}: {}", callback, preLoadContext);
            return true;
        }

        commandStore.checkInStoreThread();
        logger.trace("Running append for {} with state {}: {}", callback, state, preLoadContext);
        switch (state)
        {
            case INITIALIZED:
                commandStore.appendToJournal(preLoadContext, this);
                state = State.WAITING;
            case WAITING:
            case FINISHED:
                break;
        }
        logger.trace("Exiting append for {} with state {}: {}", callback, state, preLoadContext);
        return state == State.FINISHED;
    }

    @Override
    public void run()
    {
        commandStore.checkInStoreThread();
        if (state != State.WAITING)
            throw new IllegalStateException();
        state = State.FINISHED;
        callback.accept(null, null);
    }
}
