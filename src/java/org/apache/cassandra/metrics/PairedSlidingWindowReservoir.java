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

package org.apache.cassandra.metrics;

/**
 * A Reservoir-like class that tracks the last N paired measurements.
 */
public class PairedSlidingWindowReservoir
{
    private final IntIntPair[] measurements;
    private long count;

    public PairedSlidingWindowReservoir(int size)
    {
        this.measurements = new IntIntPair[size];
        this.count = 0L;
    }

    public int size()
    {
        if (this.count >= this.measurements.length)
            return this.measurements.length;

        synchronized (this)
        {
            return (int) Math.min(this.count, this.measurements.length);
        }
    }

    public synchronized void update(int value1, int value2)
    {
        this.measurements[(int) (this.count++ % this.measurements.length)] = new IntIntPair(value1, value2);
    }

    public PairedSnapshot getSnapshot()
    {
        var values = new IntIntPair[this.size()];
        System.arraycopy(this.measurements, 0, values, 0, values.length);
        return new PairedSnapshot(values);
    }

    /**
     * A pair of ints.  "y" and "x" are used to imply that the first value is the
     * dependent one for a LinearFit computation.
     */
    public static class IntIntPair
    {
        public final int x;
        public final int y;

        IntIntPair(int x, int y)
        {
            this.y = y;
            this.x = x;
        }
    }

    public static class PairedSnapshot
    {
        public final IntIntPair[] values;

        public PairedSnapshot(IntIntPair[] values)
        {
            this.values = values;
        }
    }
}
