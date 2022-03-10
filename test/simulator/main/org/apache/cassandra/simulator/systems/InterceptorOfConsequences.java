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

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptedWait.Trigger;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface InterceptorOfConsequences
{
    public static final InterceptorOfConsequences DEFAULT_INTERCEPTOR = new InterceptorOfConsequences()
    {
        @Override
        public void beforeInvocation(InterceptibleThread realThread)
        {
        }

        @Override
        public void interceptMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
        {
            throw new AssertionError();
        }

        @Override
        public void interceptWait(InterceptedWait wakeupWith)
        {
            throw new AssertionError();
        }

        @Override
        public void interceptWakeup(InterceptedWait wakeup, Trigger trigger, InterceptorOfConsequences waitWasInterceptedBy)
        {
            // TODO (now): should we be asserting here?
            wakeup.triggerBypass();
        }

        @Override
        public void interceptExecution(InterceptedExecution invoke, OrderOn orderOn)
        {
            throw new AssertionError();
        }

        @Override
        public void interceptTermination(boolean isThreadTermination)
        {
            throw new AssertionError();
        }
    };


    void beforeInvocation(InterceptibleThread realThread);
    void interceptMessage(IInvokableInstance from, IInvokableInstance to, IMessage message);
    void interceptWakeup(InterceptedWait wakeup, Trigger trigger, InterceptorOfConsequences waitWasInterceptedBy);
    void interceptExecution(InterceptedExecution invoke, OrderOn orderOn);
    void interceptWait(InterceptedWait wakeupWith);
    void interceptTermination(boolean isThreadTermination);
}
