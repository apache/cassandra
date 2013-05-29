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
package org.apache.cassandra.repair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
*/
public abstract class RequestCoordinator<R>
{
    private final Order<R> orderer;

    public RequestCoordinator(boolean isSequential)
    {
        this.orderer = isSequential ? new SequentialOrder(this) : new ParallelOrder(this);
    }

    public abstract void send(R request);

    public void add(R request)
    {
        orderer.add(request);
    }

    public void start()
    {
        orderer.start();
    }

    // Returns how many request remains
    public int completed(R request)
    {
        return orderer.completed(request);
    }

    private static abstract class Order<R>
    {
        protected final RequestCoordinator<R> coordinator;

        Order(RequestCoordinator<R> coordinator)
        {
            this.coordinator = coordinator;
        }

        public abstract void add(R request);
        public abstract void start();
        public abstract int completed(R request);
    }

    private static class SequentialOrder<R> extends Order<R>
    {
        private final Queue<R> requests = new LinkedList<>();

        SequentialOrder(RequestCoordinator<R> coordinator)
        {
            super(coordinator);
        }

        public void add(R request)
        {
            requests.add(request);
        }

        public void start()
        {
            if (requests.isEmpty())
                return;

            coordinator.send(requests.peek());
        }

        public int completed(R request)
        {
            assert request.equals(requests.peek());
            requests.poll();
            int remaining = requests.size();
            if (remaining != 0)
                coordinator.send(requests.peek());
            return remaining;
        }
    }

    private static class ParallelOrder<R> extends Order<R>
    {
        private final Set<R> requests = new HashSet<>();

        ParallelOrder(RequestCoordinator<R> coordinator)
        {
            super(coordinator);
        }

        public void add(R request)
        {
            requests.add(request);
        }

        public void start()
        {
            for (R request : requests)
                coordinator.send(request);
        }

        public int completed(R request)
        {
            requests.remove(request);
            return requests.size();
        }
    }

}
