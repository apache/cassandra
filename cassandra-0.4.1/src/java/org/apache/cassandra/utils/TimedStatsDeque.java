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
package org.apache.cassandra.utils;

import java.util.Iterator;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** threadsafe. */
public class TimedStatsDeque extends AbstractStatsDeque
{
    private final ArrayDeque<Tuple> deque;
    private final long period;

    public TimedStatsDeque(long period)
    {
        this.period = period;
        deque = new ArrayDeque<Tuple>();
    }

    private void purge()
    {
        long now = System.currentTimeMillis();
        while (!deque.isEmpty() && deque.peek().timestamp < now - period)
        {
            deque.remove();
        }
    }

    public synchronized Iterator<Double> iterator()
    {
        purge();
        // I expect this method to be called relatively infrequently so inefficiency is ok.
        // (this has the side benefit of making iteration threadsafe w/o having to use LinkedBlockingDeque.)
        List<Double> L = new ArrayList<Double>(deque.size());
        for (Tuple t : deque)
        {
            L.add(t.value);
        }
        return L.iterator();
    }

    public synchronized int size()
    {
        purge();
        return deque.size();
    }

    public synchronized void add(double o)
    {
        purge();
        deque.add(new Tuple(o, System.currentTimeMillis()));
    }

    public synchronized void clear()
    {
        deque.clear();
    }
}

class Tuple
{
    public final double value;
    public final long timestamp;

    public Tuple(double value, long timestamp)
    {
        this.value = value;
        this.timestamp = timestamp;
    }
}