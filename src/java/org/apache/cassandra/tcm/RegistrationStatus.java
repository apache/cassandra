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

package org.apache.cassandra.tcm;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;

public class RegistrationStatus
{
    public enum State { INITIAL, UNREGISTERED, REGISTERED }

    private static final Logger logger = LoggerFactory.getLogger(RegistrationStatus.class);
    public static final RegistrationStatus instance = new RegistrationStatus();
    private final AtomicReference<RegistrationStatus.State> state = new AtomicReference<>(State.INITIAL);

    public RegistrationStatus.State getCurrent()
    {
        return state.get();
    }

    @VisibleForTesting
    public void resetState()
    {
        state.set(State.INITIAL);
    }

    public void onInitialized()
    {
        logger.info("Node is initialized, moving to UNREGISTERED state");
        if (!state.compareAndSet(State.INITIAL, State.UNREGISTERED))
            throw new IllegalStateException(String.format("Cannot move to UNREGISTERED state (%s)", state.get()));
    }

    public void onRegistration()
    {
        // This may have been done already if the metadata log replay at start up included our registration
        RegistrationStatus.State current = state.get();
        if (current == State.REGISTERED)
            return;

        logger.info("This node is registered, moving state to REGISTERED and interrupting any previously established peer connections");
        state.getAndSet(RegistrationStatus.State.REGISTERED);
        MessagingService.instance().channelManagers.keySet().forEach(MessagingService.instance()::interruptOutbound);
    }
}
