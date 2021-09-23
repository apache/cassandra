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
package org.apache.cassandra.service;

import javax.annotation.Nullable;

/**
 * As tracing can happen at both coordinator and replicas, at replica side, CNDB needs to know the traced keyspace for billing
 */
public class TracingClientState extends ClientState
{
    private final @Nullable String tracedKeyspace;

    protected TracingClientState(String tracedKeyspace, ClientState state)
    {
        super(state);
        this.tracedKeyspace = tracedKeyspace;
    }

    @Override
    public ClientState cloneWithKeyspaceIfSet(String keyspace)
    {
        if (keyspace == null)
            return this;
        return new TracingClientState(tracedKeyspace, super.cloneWithKeyspaceIfSet(keyspace));
    }

    /**
     * @return the keyspace being traced
     */
    @Nullable
    public String tracedKeyspace()
    {
        return tracedKeyspace;
    }

    /**
     * @return a ClientState object for internal C* calls (not limited by any kind of auth) with traced keyspace
     */
    public static TracingClientState withTracedKeyspace(@Nullable String tracedKeyspace)
    {
        return new TracingClientState(tracedKeyspace, ClientState.forInternalCalls());
    }
}
