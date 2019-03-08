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

package org.apache.cassandra.auth;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.service.QueryState;

public class AuthEvents
{
    private static final Logger logger = LoggerFactory.getLogger(QueryEvents.class);

    public static final AuthEvents instance = new AuthEvents();

    private final Set<Listener> listeners = new CopyOnWriteArraySet<>();

    @VisibleForTesting
    public int listenerCount()
    {
        return listeners.size();
    }

    public void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    public void unregisterListener(Listener listener)
    {
        listeners.remove(listener);
    }

    public void notifyAuthSuccess(QueryState state)
    {
        try
        {
            for (Listener listener : listeners)
                listener.authSuccess(state);
        }
        catch (Exception e)
        {
            logger.error("Failed notifying listeners", e);
        }
    }

    public void notifyAuthFailure(QueryState state, Exception cause)
    {
        try
        {
            for (Listener listener : listeners)
                listener.authFailure(state, cause);
        }
        catch (Exception e)
        {
            logger.error("Failed notifying listeners", e);
        }
    }

    public static interface Listener
    {
        void authSuccess(QueryState state);
        void authFailure(QueryState state, Exception cause);
    }
}
