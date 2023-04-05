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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.Duration;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Base type for temporal types (timestamp, date ...).
 *
 */
public abstract class TemporalType<T> extends AbstractType<T>
{
    protected TemporalType(ComparisonType comparisonType)
    {
        super(comparisonType);
    }

    /**
     * Returns the current temporal value.
     * @return the current temporal value.
     */
    public ByteBuffer now()
    {
        return fromTimeInMillis(currentTimeMillis());
    }

    /**
     * Converts this temporal in UNIX timestamp.
     * @param value the temporal value.
     * @return the UNIX timestamp corresponding to this temporal.
     */
    public long toTimeInMillis(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the temporal value corresponding to the specified UNIX timestamp.
     * @param timeInMillis the UNIX timestamp to convert
     * @return the temporal value corresponding to the specified UNIX timestamp
     */
    public ByteBuffer fromTimeInMillis(long timeInMillis)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds the duration to the specified value.
     *
     * @param temporal the value to add to
     * @param duration the duration to add
     * @return the addition result
     */
    public ByteBuffer addDuration(ByteBuffer temporal,
                                  ByteBuffer duration)
    {
        long timeInMillis = toTimeInMillis(temporal);
        Duration d = DurationType.instance.compose(duration);
        validateDuration(d);
        return fromTimeInMillis(d.addTo(timeInMillis));
    }

    /**
     * Substract the duration from the specified value.
     *
     * @param temporal the value to substract from
     * @param duration the duration to substract
     * @return the substracion result
     */
    public ByteBuffer substractDuration(ByteBuffer temporal,
                                ByteBuffer duration)
    {
        long timeInMillis = toTimeInMillis(temporal);
        Duration d = DurationType.instance.compose(duration);
        validateDuration(d);
        return fromTimeInMillis(d.substractFrom(timeInMillis));
    }

    /**
     * Validates that the duration has the correct precision.
     * @param duration the duration to validate.
     */
    protected void validateDuration(Duration duration)
    {
    }
}
