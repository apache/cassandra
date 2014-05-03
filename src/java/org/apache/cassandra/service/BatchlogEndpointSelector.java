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
package org.apache.cassandra.service;


import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class BatchlogEndpointSelector
{
    private final String localRack;
    
    public BatchlogEndpointSelector(String localRack)
    {
        this.localRack = localRack;
    }

    /**
     * @param endpoints nodes in the local datacenter, grouped by rack name
     * @return list of candidates for batchlog hosting.  if possible these will be two nodes from different racks.
     */
    public Collection<InetAddress> chooseEndpoints(Multimap<String, InetAddress> endpoints)
    {
        // strip out dead endpoints and localhost
        ListMultimap<String, InetAddress> validated = ArrayListMultimap.create();
        for (Map.Entry<String, InetAddress> entry : endpoints.entries())
        {
            if (isValid(entry.getValue()))
                validated.put(entry.getKey(), entry.getValue());
        }
        if (validated.size() <= 2)
            return validated.values();

        if ((validated.size() - validated.get(localRack).size()) >= 2)
        {
            // we have enough endpoints in other racks
            validated.removeAll(localRack);
        }

        if (validated.keySet().size() == 1)
        {
            // we have only 1 `other` rack
            Collection<InetAddress> otherRack = Iterables.getOnlyElement(validated.asMap().values());
            return Lists.newArrayList(Iterables.limit(otherRack, 2));
        }

        // randomize which racks we pick from if more than 2 remaining
        Collection<String> racks;
        if (validated.keySet().size() == 2)
        {
            racks = validated.keySet();
        }
        else
        {
            racks = Lists.newArrayList(validated.keySet());
            Collections.shuffle((List) racks);
        }

        // grab a random member of up to two racks
        List<InetAddress> result = new ArrayList<>(2);
        for (String rack : Iterables.limit(racks, 2))
        {
            List<InetAddress> rackMembers = validated.get(rack);
            result.add(rackMembers.get(getRandomInt(rackMembers.size())));
        }

        return result;
    }
    
    @VisibleForTesting
    protected boolean isValid(InetAddress input)
    {
        return !input.equals(FBUtilities.getBroadcastAddress()) && FailureDetector.instance.isAlive(input);
    }
    
    @VisibleForTesting
    protected int getRandomInt(int bound)
    {
        return FBUtilities.threadLocalRandom().nextInt(bound);
    }
}
