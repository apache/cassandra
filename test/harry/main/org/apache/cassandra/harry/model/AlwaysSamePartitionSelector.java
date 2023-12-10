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

package org.apache.cassandra.harry.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;

/**
 * A simple test-only descriptor selector that can used for testing things where you only need one partition
 */
public class AlwaysSamePartitionSelector extends OpSelectors.PdSelector
{
    private final long pd;

    public AlwaysSamePartitionSelector(long pd)
    {
        this.pd = pd;
    }

    protected long pd(long lts)
    {
        return 0;
    }

    public long nextLts(long lts)
    {
        return lts + 1;
    }

    public long prevLts(long lts)
    {
        return lts - 1;
    }

    public long maxLtsFor(long pd)
    {
        return 1000;
    }

    public long minLtsAt(long position)
    {
        return 0;
    }

    public long minLtsFor(long pd)
    {
        return 0;
    }

    public long positionFor(long lts)
    {
        return 0;
    }

    public long maxPosition(long maxLts)
    {
        return 0;
    }

    @JsonTypeName("always_same")
    public static class AlwaysSamePartitionSelectorConfiguration implements Configuration.PDSelectorConfiguration
    {
        private final long pd;

        public AlwaysSamePartitionSelectorConfiguration(@JsonProperty("pd") long pd)
        {
            this.pd = pd;
        }

        public OpSelectors.PdSelector make(OpSelectors.PureRng rng)
        {
            return new AlwaysSamePartitionSelector(pd);
        }
    }
}
