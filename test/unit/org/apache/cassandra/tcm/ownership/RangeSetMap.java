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

package org.apache.cassandra.tcm.ownership;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.ReplicationParams;

public class RangeSetMap extends ReplicationMap<Set<Range<Token>>>
{
    public RangeSetMap()
    {
        super(new HashMap<>());
    }

    public RangeSetMap(Map<ReplicationParams, Set<Range<Token>>> map)
    {
        super(map);
    }

    @Override
    protected Set<Range<Token>> defaultValue()
    {
        return Set.of();
    }

    @Override
    protected Set<Range<Token>> localOnly()
    {
        throw new UnsupportedOperationException();
    }

    public Builder unbuild()
    {
        return new Builder(map);
    }

    public void clear()
    {
        map.clear();
    }

    public String toString()
    {
        return "RangeSetMap{" +
               "map=" + map +
               '}';
    }

    public static Builder builder()
    {
        return new Builder(new HashMap<>());
    }

    public static Builder builder(int expectedSize)
    {
        return new Builder(Maps.newHashMapWithExpectedSize(expectedSize));
    }

    public static class Builder
    {
        private final Map<ReplicationParams, Set<Range<Token>>> map;
        private Builder(Map<ReplicationParams, Set<Range<Token>>> map)
        {
            this.map = map;
        }

        public Builder put(ReplicationParams params, Set<Range<Token>> ranges)
        {
            map.put(params, ranges);
            return this;
        }

        public RangeSetMap build()
        {
            return new RangeSetMap(map);
        }
    }
}
