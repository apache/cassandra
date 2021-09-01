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

import java.util.Map;
import java.util.function.Supplier;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.OrderOn;

public class SimulatedActionTaskDroppable extends SimulatedActionTask
{
    final Supplier<ActionList> drop;

    public SimulatedActionTaskDroppable(Object description, Kind kind, OrderOn orderOn, Modifiers self, Modifiers transitive, Map<Verb, Modifiers> verbModifiers, SimulatedSystems simulated, InterceptedExecution task, Supplier<ActionList> drop)
    {
        super(description, kind, orderOn, self, transitive, verbModifiers, simulated, task);
        this.drop = drop;
    }

    protected ActionList perform(boolean perform)
    {
        if (perform) return super.perform(true);
        else return performed(this.drop.get(), true, true);
    }
}
