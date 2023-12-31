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

package org.apache.cassandra.harry.clock;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.model.OpSelectors;

/**
 * A trivial implemementation of the clock, one that does not attempt to follow the wall clock.
 * This clock simply offsets LTS by a preset value.
 */
public class OffsetClock implements OpSelectors.Clock
{
    final AtomicLong lts;

    private final long base;

    public OffsetClock(long base)
    {
        this(ApproximateClock.START_VALUE, base);
    }

    public OffsetClock(long startValue, long base)
    {
        this.lts = new AtomicLong(startValue);
        this.base = base;
    }

    public long rts(long lts)
    {
        return base + lts;
    }

    public long lts(long rts)
    {
        return rts - base;
    }

    public long nextLts()
    {
        return lts.getAndIncrement();
    }

    public long peek()
    {
        return lts.get();
    }

    public Configuration.ClockConfiguration toConfig()
    {
        return new OffsetClockConfiguration(lts.get(), base);
    }

    @JsonTypeName("offset")
    public static class OffsetClockConfiguration implements Configuration.ClockConfiguration
    {
        public final long offset;
        public final long base;

        @JsonCreator
        public OffsetClockConfiguration(@JsonProperty("offset") long offset,
                                        @JsonProperty(value = "base", defaultValue = "0") long base)
        {
            this.offset = offset;
            this.base = base;
        }

        public OpSelectors.Clock make()
        {
            return new OffsetClock(base, offset);
        }
    }
}
