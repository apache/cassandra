/**
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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A simple endpoint snitch implementation that treats Strategy order as proximity,
 * allowing non-read-repaired reads to prefer a single endpoint, which improves
 * cache locality.
 */
public class SimpleSnitch extends AbstractEndpointSnitch
{
    public String getRack(InetAddress endpoint)
    {
        return "rack1";
    }

    public String getDatacenter(InetAddress endpoint)
    {
        return "datacenter1";
    }

    @Override
    public void sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        // Optimization to avoid walking the list
    }

    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
    {
        // Making all endpoints equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }
}
