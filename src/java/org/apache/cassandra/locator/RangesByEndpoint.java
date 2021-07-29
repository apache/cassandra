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

package org.apache.cassandra.locator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

public class RangesByEndpoint extends ReplicaMultimap<InetAddressAndPort, RangesAtEndpoint>
{
    public RangesByEndpoint(Map<InetAddressAndPort, RangesAtEndpoint> map)
    {
        super(map);
    }

    public RangesAtEndpoint get(InetAddressAndPort endpoint)
    {
        Preconditions.checkNotNull(endpoint);
        return map.getOrDefault(endpoint, RangesAtEndpoint.empty(endpoint));
    }

    public static class Builder extends ReplicaMultimap.Builder<InetAddressAndPort, RangesAtEndpoint.Builder>
    {
        @Override
        protected RangesAtEndpoint.Builder newBuilder(InetAddressAndPort endpoint)
        {
            return new RangesAtEndpoint.Builder(endpoint);
        }

        public RangesByEndpoint build()
        {
            return new RangesByEndpoint(
                    ImmutableMap.copyOf(
                            Maps.transformValues(this.map, RangesAtEndpoint.Builder::build)));
        }
    }

}
