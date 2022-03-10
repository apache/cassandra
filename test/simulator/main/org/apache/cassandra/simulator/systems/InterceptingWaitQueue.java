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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.cassandra.simulator.systems.InterceptingAwaitable.InterceptingSignal;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods.Global.ifIntercepted;

@PerClassLoader
class InterceptingWaitQueue extends WaitQueue.Standard implements WaitQueue
{
    final Queue<InterceptingSignal<?>> interceptible = new ConcurrentLinkedQueue<>();

    public InterceptingWaitQueue()
    {
    }

    public Signal register()
    {
        if (ifIntercepted() == null)
            return super.register();

        InterceptingSignal<?> signal = new InterceptingSignal<>();
        interceptible.add(signal);
        return signal;
    }

    public <V> Signal register(V value, Consumer<V> consumer)
    {
        if (ifIntercepted() == null)
            return super.register(value, consumer);

        InterceptingSignal<V> signal = new InterceptingSignal<>(value, consumer);
        interceptible.add(signal);
        return signal;
    }

    public boolean signal()
    {
        // directly signal the actual underlying queue if no intercepted waiters are present
        return consumeUntil(InterceptingSignal::doSignal) || super.signal();
    }

    public void signalAll()
    {
        consumeUntil(s -> {
            s.signal();
            return false;
        });
        super.signalAll();
    }

    public boolean hasWaiters()
    {
        if (super.hasWaiters())
            return true;
        if (interceptible.isEmpty())
            return false;

        return !interceptible.stream().allMatch(Signal::isSet);
    }

    private boolean consumeUntil(Predicate<InterceptingSignal<?>> consumeUntil)
    {
        InterceptingSignal<?> signal;
        while (null != (signal = interceptible.poll()))
        {
            if (consumeUntil.test(signal))
                return true;
        }
        return false;
    }

    public int getWaiting()
    {
        return super.getWaiting() + (int)interceptible.stream().filter(s -> !s.isSignalled).count();
    }
}
