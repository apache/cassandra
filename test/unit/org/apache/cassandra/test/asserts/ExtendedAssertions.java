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

package org.apache.cassandra.test.asserts;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import org.assertj.core.api.AbstractObjectAssert;

public class ExtendedAssertions
{
    public static CounterAssert assertThat(Counter counter)
    {
        return new CounterAssert(counter);
    }

    public static HistogramAssert assertThat(Histogram histogram)
    {
        return new HistogramAssert(histogram);
    }

    public static abstract class CountingAssert<T extends Counting, Self extends CountingAssert<T, Self>> extends AbstractObjectAssert<Self, T>
    {
        protected CountingAssert(T t, Class<?> selfType)
        {
            super(t, selfType);
        }

        public Self hasCount(int expected)
        {
            isNotNull();
            if (actual.getCount() != expected)
                throw failure("%s count was %d, but expected %d", actual.getClass().getSimpleName(), actual.getCount(), expected);
            return (Self) this;
        }

        public Self isEmpty()
        {
            return hasCount(0);
        }
    }

    public static class CounterAssert extends CountingAssert<Counter, CounterAssert>
    {
        public CounterAssert(Counter counter)
        {
            super(counter, CounterAssert.class);
        }
    }

    public static class HistogramAssert extends CountingAssert<Histogram, HistogramAssert>
    {
        public HistogramAssert(Histogram histogram)
        {
            super(histogram, HistogramAssert.class);
        }

        public HistogramAssert hasMax(long expected)
        {
            isNotNull();
            Snapshot snapshot = actual.getSnapshot();
            if (snapshot.getMax() != expected)
                throw failure("Expected max %d but given %d", expected, actual.getCount());
            return this;
        }
    }
}
