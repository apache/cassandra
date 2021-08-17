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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Histogram;

/**
 * Adds ability to reset a histogram
 */
public class ClearableHistogram extends Histogram
{
    private final DecayingEstimatedHistogramReservoir reservoirRef;

    /**
     * Creates a new {@link com.codahale.metrics.Histogram} with the given reservoir.
     *
     * @param reservoir the reservoir to create a histogram from
     */
    public ClearableHistogram(DecayingEstimatedHistogramReservoir reservoir)
    {
        super(reservoir);

        this.reservoirRef = reservoir;
    }

    @VisibleForTesting
    public void clear()
    {
        clearCount();
        reservoirRef.clear();
    }

    private void clearCount()
    {
        // We have unfortunately no access to the count field so we need to use reflection to ensure that it is cleared.
        // I hate that as it is fragile and pretty ugly but we only use that method for tests and it should fail pretty
        // clearly if we start using an incompatible version of the metrics.
        try
        {
            Field countField = Histogram.class.getDeclaredField("count");
            countField.setAccessible(true);
            // in 3.1 the counter object is a LongAdderAdapter which is a package private interface
            // from com.codahale.metrics. In 4.0, it is a java LongAdder so the code will be simpler.
            Object counter = countField.get(this);
            if (counter instanceof LongAdder) // For com.codahale.metrics version >= 4.0
            {
                ((LongAdder) counter).reset();
            }
            else // 3.1 and 3.2
            {
                Method sumThenReset = counter.getClass().getDeclaredMethod("sumThenReset");
                sumThenReset.setAccessible(true);
                sumThenReset.invoke(counter);
            }
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Cannot reset the com.codahale.metrics.Histogram count. This might be due to a change of version of the metric library", e);
        }
    }
}
