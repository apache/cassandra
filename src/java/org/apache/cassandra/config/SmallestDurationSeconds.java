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

package org.apache.cassandra.config;

import java.util.concurrent.TimeUnit;

/**
 * Wrapper class for Cassandra duration configuration parameters which are internally represented in Seconds. In order
 * not to lose precision while converting to smaller units (until we migrate those parameters to use internally the smallest
 * supported unit) we restrict those parameters to use only Seconds or larger units. (CASSANDRA-15234)
 */
public final class SmallestDurationSeconds extends DurationSpec
{
    /**
     * Creates a {@code SmallestDurationSeconds} of the specified amount of seconds and provides the smallest
     * required unit of seconds for the respective parameter of type {@code SmallestDurationSeconds}.
     *
     * @param value the duration
     *
     */
    public SmallestDurationSeconds(String value)
    {
        super(value, TimeUnit.SECONDS);
    }

    private SmallestDurationSeconds(long quantity, TimeUnit unit)
    {
        super(quantity, unit);
    }

    /**
     * Creates a {@code SmallestDurationSeconds} of the specified amount of seconds.
     *
     * @param seconds the amount of seconds
     * @return a duration
     */
    public static SmallestDurationSeconds inSeconds(long seconds)
    {
        return new SmallestDurationSeconds(seconds, TimeUnit.SECONDS);
    }
}
