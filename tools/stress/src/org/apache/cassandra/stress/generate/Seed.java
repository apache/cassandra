/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.stress.generate;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.stress.util.DynamicList;

public class Seed implements Comparable<Seed>
{

    public final long seed;
    final int visits;

    DynamicList.Node poolNode;
    volatile int[] position;
    volatile State state = State.HELD;

    private static final AtomicReferenceFieldUpdater<Seed, Seed.State> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(Seed.class, State.class, "state");

    public int compareTo(Seed that)
    {
        return Long.compare(this.seed, that.seed);
    }

    static enum State
    {
        HELD, AVAILABLE
    }

    Seed(long seed, int visits)
    {
        this.seed = seed;
        this.visits = visits;
    }

    boolean take()
    {
        return stateUpdater.compareAndSet(this, State.AVAILABLE, State.HELD);
    }

    void yield()
    {
        state = State.AVAILABLE;
    }

    public int[] position()
    {
        return position;
    }
}
