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

package org.apache.cassandra.simulator.systems;

import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Query;

public class SimulatedQuery extends SimulatedActionCallable<SimpleQueryResult>
{
    private static final Predicate<Throwable> DEFAULT_PREDICATE = i -> true;

    private final Predicate<Throwable> onFailure;

    public SimulatedQuery(Object description, SimulatedSystems simulated, IInvokableInstance instance, String query, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, Object... params)
    {
        this(description, Modifiers.NONE, Modifiers.NONE, simulated, instance, query, commitConsistency, serialConsistency, null, params);
    }

    public SimulatedQuery(Object description, SimulatedSystems simulated, IInvokableInstance instance, String query, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, @Nullable Predicate<Throwable> onFailure, Object... params)
    {
        this(description, Modifiers.NONE, Modifiers.NONE, simulated, instance, query, commitConsistency, serialConsistency, onFailure, params);
    }

    private SimulatedQuery(Object description, Modifiers self, Modifiers transitive, SimulatedSystems simulated, IInvokableInstance instance, String query, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, @Nullable Predicate<Throwable> onFailure, Object[] params)
    {
        super(description, self, transitive, simulated, instance, new Query(query, -1, commitConsistency, serialConsistency, params));
        this.onFailure = onFailure == null ? DEFAULT_PREDICATE : onFailure;
    }

    public SimulatedQuery(Object description, Modifiers self, Modifiers transitive, SimulatedSystems simulated, IInvokableInstance instance, String query, long timestamp, ConsistencyLevel consistency, Object... params)
    {
        this(description, self, transitive, simulated, instance, query, timestamp, consistency, null, null, params);
    }

    private SimulatedQuery(Object description, Modifiers self, Modifiers transitive, SimulatedSystems simulated, IInvokableInstance instance, String query, long timestamp, ConsistencyLevel commitConsistency, ConsistencyLevel serialConsistency, @Nullable Predicate<Throwable> onFailure, Object[] params)
    {
        super(description, self, transitive, simulated, instance, new Query(query, timestamp, commitConsistency, serialConsistency, params));
        this.onFailure = onFailure == null ? DEFAULT_PREDICATE : onFailure;
    }

    @Override
    public void accept(SimpleQueryResult success, Throwable failure)
    {
        if (failure != null && onFailure.test(failure))
            simulated.failures.accept(failure);
    }
}
