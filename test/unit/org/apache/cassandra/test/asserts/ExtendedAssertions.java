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

import com.codahale.metrics.Counting;
import org.assertj.core.api.AbstractObjectAssert;

public class ExtendedAssertions
{
    public static CountingAssert assertThat(Counting counter)
    {
        return new CountingAssert(counter);
    }

    public static class CountingAssert extends AbstractObjectAssert<CountingAssert, Counting>
    {
        public CountingAssert(Counting histogram)
        {
            super(histogram, CountingAssert.class);
        }

        public CountingAssert hasCount(int expected)
        {
            isNotNull();
            if (actual.getCount() != expected)
                throw failure("%s count was %d, but expected %d", actual.getClass().getSimpleName(), actual.getCount(), expected);
            return this;
        }

        public CountingAssert isEmpty()
        {
            return hasCount(0);
        }
    }
}
