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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableBiFunction;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites;
import org.apache.cassandra.utils.Closeable;

import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;

public class SimulatedWaits implements InterceptorOfWaits
{
    private static final InterceptorOfConsequences DEFAULT_INTERCEPTOR = new InterceptorOfConsequences()
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
        public void interceptWakeup(InterceptedWait wakeup, InterceptorOfConsequences waitWasInterceptedBy)
        {
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

    private final RandomSource random;
    private boolean disabled = false;
    private boolean captureWaitSites, captureWakeSites, captureNowSites;
    private final List<Closeable> onStop = Collections.synchronizedList(new ArrayList<>());

    public SimulatedWaits(RandomSource random)
    {
        this.random = random;
    }

    public InterceptorOfGlobalMethods interceptGlobalMethods(ClassLoader classLoader)
    {
        InterceptorOfGlobalMethods interceptor = IsolatedExecutor.transferAdhoc((SerializableBiFunction<InterceptorOfWaits, RandomSource, InterceptorOfGlobalMethods>) InterceptingGlobalMethods::new, classLoader).apply(this, random);
        onStop.add(interceptor);
        return interceptor;
    }

    public CaptureSites captureWaitSite(Thread thread)
    {
        return captureWaitSites ? new CaptureSites(thread, captureNowSites) : captureWakeSites | captureNowSites ? new CaptureSites(thread, null, captureNowSites) : null;
    }

    private void captureWakeSite(Thread thread, CaptureSites captureSites)
    {
        if (captureSites != null && captureWakeSites)
            captureSites.registerWakeup(thread);
    }

    @Override
    public void interceptSignal(Thread signalledBy, InterceptedWait signalled, CaptureSites waitSites, InterceptorOfConsequences interceptedBy)
    {
        captureWakeSite(signalledBy, waitSites);
        interceptorOrDefault(signalledBy).interceptWakeup(signalled, interceptedBy);
    }

    private InterceptorOfConsequences interceptorOrDefault(Thread thread)
    {
        if (!(thread instanceof InterceptibleThread))
            return DEFAULT_INTERCEPTOR;

        return interceptorOrDefault((InterceptibleThread) thread);
    }

    private InterceptorOfConsequences interceptorOrDefault(InterceptibleThread thread)
    {
        return thread.isIntercepting() ? thread : DEFAULT_INTERCEPTOR;
    }

    public InterceptibleThread ifIntercepted()
    {
        Thread thread = Thread.currentThread();
        if (thread instanceof InterceptibleThread)
        {
            InterceptibleThread interceptibleThread = (InterceptibleThread) thread;
            if (interceptibleThread.isIntercepting())
                return interceptibleThread;
        }

        if (NonInterceptible.isPermitted())
            return null;

        if (!disabled)
            throw failWithOOM();

        return null;
    }

    public void stop()
    {
        this.disabled = true;
        onStop.forEach(Closeable::close);
        // TODO (cleanup): signal remaining waiters for cleaner shutdown?
    }

    public void captureStackTraces(boolean waitSites, boolean wakeSites, boolean nowSites)
    {
        captureWaitSites = waitSites;
        captureWakeSites = wakeSites;
        captureNowSites = nowSites;
    }
}
