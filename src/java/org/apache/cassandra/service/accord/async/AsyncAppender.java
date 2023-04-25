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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.PreLoadContext;
import accord.utils.Invariants;
import org.apache.cassandra.journal.AsyncWriteCallback;
import org.apache.cassandra.service.accord.AccordCommandStore;

/**
 * Durably appends PreAccept, Accept, Commit, and Apply messages to {@code AccordJournal}
 * before any further steps can be attempted.
 */
public class AsyncAppender implements AsyncWriteCallback
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
        commandStore.checkInStoreThread();

        if (!commandStore.mustAppendToJournal(preLoadContext))
        {
            logger.trace("Skipping append for {}: {}", callback, preLoadContext);
            return true;
        }

        logger.trace("Running append for {} with state {}: {}", callback, state, preLoadContext);
        if (state == State.INITIALIZED)
        {
            commandStore.appendToJournal(preLoadContext, this);
            state = State.WAITING;
        }
        logger.trace("Exiting append for {} with state {}: {}", callback, state, preLoadContext);

        return state == State.FINISHED;
    }

    private void onFinished(@Nullable Throwable error)
    {
        commandStore.checkInStoreThread();
        Invariants.checkState(state == State.WAITING, "Expected WAITING state but was %s", state);
        state = State.FINISHED;
        callback.accept(null, error);
    }

    @Override
    public void onSuccess()
    {
        onFinished(null);
    }

    @Override
    public void onError(Throwable error)
    {
        onFinished(error);
    }
}
