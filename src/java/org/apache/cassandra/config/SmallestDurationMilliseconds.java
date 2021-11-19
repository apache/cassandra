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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.commons.lang.StringUtils;

/**
 * Wrapper class for Cassandra duration configuration parameters which are internally represented in Milliseconds. In order
 * not to lose precision while converting to smaller units (until we migrate those parameters to use internally the smallest
 * supported unit) we restrict those parameters to use only Milliseconds or larger units. (CASSANDRA-15234)
 */
public final class SmallestDurationMilliseconds extends DurationSpec
{
    /**
     * Creates a {@code SmallestDurationMilliseconds} of the specified amount of milliseconds and provides the smallest
     * required unit of milliseconds for the respective parameter of type {@code SmallestDurationMilliseconds}.
     *
     * @param value the duration
     *
     */
    public SmallestDurationMilliseconds(String value)
    {
        super(value, TimeUnit.MILLISECONDS);
    }

    private SmallestDurationMilliseconds(long quantity, TimeUnit unit)
    {
        super(quantity, unit);
    }

    private SmallestDurationMilliseconds(double quantity, TimeUnit unit)
    {
        super(quantity, unit);
    }

    /**
     * Creates a {@code SmallestDurationMilliseconds} of the specified amount of milliseconds.
     *
     * @param milliseconds the amount of milliseconds
     * @return a duration
     */
    public static SmallestDurationMilliseconds inMilliseconds(long milliseconds)
    {
        return new SmallestDurationMilliseconds(milliseconds, TimeUnit.MILLISECONDS);
    }

    public static SmallestDurationMilliseconds inDoubleMilliseconds(double milliseconds)
    {
        return new SmallestDurationMilliseconds(milliseconds, MILLISECONDS);
    }

    /**
     * Creates a {@code SmallestDurationMilliseconds} of the specified amount of milliseconds. Custom method for special cases.
     *
     * @param value may be a simple number (assumed to be in milliseconds) or representation the constructor can handle
     * @return a duration
     */
    public static SmallestDurationMilliseconds inMillisecondsString(String value)
    {
        //parse the string field value
        if (StringUtils.isNumeric(value))
            return inMilliseconds(Long.parseLong(value));

        //otherwise we just use the standard constructors
        return new SmallestDurationMilliseconds(value);
    }
}
