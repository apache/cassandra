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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.stress.util.DynamicList;

public class Seed implements Comparable<Seed>
{

    public final int visits;
    public final long seed;

    private volatile DynamicList.Node poolNode;
    private volatile int position;

    private static final AtomicIntegerFieldUpdater<Seed> positionUpdater = AtomicIntegerFieldUpdater.newUpdater(Seed.class, "position");

    public int compareTo(Seed that)
    {
        return Long.compare(this.seed, that.seed);
    }

    Seed(long seed, int visits)
    {
        this.seed = seed;
        this.visits = visits;
    }

    public int position()
    {
        return position;
    }

    public int moveForwards(int rowCount)
    {
        return positionUpdater.getAndAdd(this, rowCount);
    }

    public int hashCode()
    {
        return (int) seed;
    }

    public boolean equals(Object that)
    {
        return that instanceof Seed && this.seed == ((Seed) that).seed;
    }

    public boolean save(DynamicList<Seed> sampleFrom, int maxSize)
    {
        DynamicList.Node poolNode = sampleFrom.append(this, maxSize);
        if (poolNode == null)
            return false;
        this.poolNode = poolNode;
        return true;
    }

    public boolean isSaved()
    {
        return poolNode != null;
    }

    public void remove(DynamicList<Seed> sampleFrom)
    {
        sampleFrom.remove(poolNode);
    }
}
