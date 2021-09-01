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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;

public class SimulatedFailureDetector
{
    public static class Instance implements IFailureDetector
    {
        private static volatile FailureDetector wrapped;

        private static volatile Function<InetSocketAddress, Boolean> OVERRIDE;
        private static final Map<IFailureDetectionEventListener, Boolean> LISTENERS = Collections.synchronizedMap(new IdentityHashMap<>());

        private static FailureDetector wrapped()
        {
            FailureDetector detector = wrapped;
            if (detector == null)
            {
                synchronized (LISTENERS)
                {
                    if (wrapped == null)
                        wrapped = new FailureDetector();
                }
                detector = wrapped;
            }
            return detector;
        }

        private Boolean override(InetAddressAndPort ep)
        {
            Function<InetSocketAddress, Boolean> overrideF = OVERRIDE;
            return overrideF == null ? null : overrideF.apply(new InetSocketAddress(ep.getAddress(), ep.getPort()));
        }

        public boolean isAlive(InetAddressAndPort ep)
        {
            Boolean override = override(ep);
            return override != null ? override : wrapped().isAlive(ep);
        }

        public void interpret(InetAddressAndPort ep)
        {
            wrapped().interpret(ep);
        }

        public void report(InetAddressAndPort ep)
        {
            wrapped().report(ep);
        }

        public void remove(InetAddressAndPort ep)
        {
            wrapped().remove(ep);
        }

        public void forceConviction(InetAddressAndPort ep)
        {
            wrapped().forceConviction(ep);
        }

        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            LISTENERS.put(listener, Boolean.TRUE);
        }

        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            LISTENERS.remove(listener);
        }

        synchronized static void setup(Function<InetSocketAddress, Boolean> override, Consumer<Consumer<InetSocketAddress>> register)
        {
            OVERRIDE = override;
            register.accept(ep -> LISTENERS.keySet().forEach(c -> c.convict(InetAddressAndPort.getByAddress(ep), Double.MAX_VALUE)));
        }
    }

    final List<Consumer<InetSocketAddress>> listeners = new CopyOnWriteArrayList<>();
    final Map<InetSocketAddress, Boolean> override = new ConcurrentHashMap<>();

    public SimulatedFailureDetector(Cluster cluster)
    {
        cluster.forEach(i -> i.unsafeAcceptOnThisThread(Instance::setup,
                override::get,
                consumer -> listeners.add(e -> i.unsafeAcceptOnThisThread(Consumer::accept, consumer, e)))
        );
    }

    public void markDown(InetSocketAddress address)
    {
        override.put(address, Boolean.FALSE);
        listeners.forEach(c -> c.accept(address));
    }
}
