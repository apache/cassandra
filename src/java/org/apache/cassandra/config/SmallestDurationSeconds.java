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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper class for Cassandra duration configuration parameters which are internally represented in Seconds. In order
 * not to lose precision while converting to smaller units (until we migrate those parameters to use internally the smallest
 * supported unit) we restrict those parameters to use only Seconds or larger units. (CASSANDRA-15234)
 */
public final class SmallestDurationSeconds extends DurationSpec
{
    private static final Pattern VALUES_PATTERN = Pattern.compile(("\\d+"));

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

    /**
     * Creates a {@code SmallestDurationSeconds} of the specified amount of seconds. Custom method for special cases.
     *
     * @param value which can be in the old form only presenting the quantity or the post CASSANDRA-15234 form - a
     * value consisting of quantity and unit. This method is necessary for three parameters which didn't change their
     * names but only their value format. (key_cache_save_period, row_cache_save_period, counter_cache_save_period)
     * @return a duration
     */
    public static SmallestDurationSeconds inSecondsString(String value)
    {
        //parse the string field value
        Matcher matcher = VALUES_PATTERN.matcher(value);

        long seconds;
        //if the provided string value is just a number, then we create a Duration Spec value in seconds
        if (matcher.matches())
        {
            seconds = Long.parseLong(value);
            return new SmallestDurationSeconds(seconds, TimeUnit.SECONDS);
        }

        //otherwise we just use the standard constructors
        return new SmallestDurationSeconds(value);
    }
}
