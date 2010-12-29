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

import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * not threadsafe.  caller is responsible for any locking necessary.
 */
public class BoundedStatsDeque extends AbstractStatsDeque
{
    private final int size;
    protected final ArrayDeque<Double> deque;

    public BoundedStatsDeque(int size)
    {
        this.size = size;
        deque = new ArrayDeque<Double>(size);
    }

    public Iterator<Double> iterator()
    {
        return deque.iterator();
    }

    public int size()
    {
        return deque.size();
    }

    public void clear()
    {
        deque.clear();
    }

    public void add(double o)
    {
        if (size == deque.size())
        {
            deque.remove();
        }
        deque.add(o);
    }
}
