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

package org.apache.cassandra.repair.asymmetric;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.TreeResponse;
import org.apache.cassandra.utils.MerkleTrees;

/**
 * Just holds all differences between the hosts involved in a repair
 */
public class DifferenceHolder
{
    private final ImmutableMap<InetAddressAndPort, HostDifferences> differences;

    public DifferenceHolder(List<TreeResponse> trees)
    {
        ImmutableMap.Builder<InetAddressAndPort, HostDifferences> diffBuilder = ImmutableMap.builder();
        for (int i = 0; i < trees.size() - 1; ++i)
        {
            TreeResponse r1 = trees.get(i);
            // create the differences between r1 and all other hosts:
            HostDifferences hd = new HostDifferences();
            for (int j = i + 1; j < trees.size(); ++j)
            {
                TreeResponse r2 = trees.get(j);
                hd.add(r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees));
            }
            r1.trees.release();
            // and add them to the diff map
            diffBuilder.put(r1.endpoint, hd);
        }
        trees.get(trees.size() - 1).trees.release();
        differences = diffBuilder.build();
    }

    @VisibleForTesting
    DifferenceHolder(Map<InetAddressAndPort, HostDifferences> differences)
    {
        ImmutableMap.Builder<InetAddressAndPort, HostDifferences> diffBuilder = ImmutableMap.builder();
        diffBuilder.putAll(differences);
        this.differences = diffBuilder.build();
    }

    /**
     * differences only holds one 'side' of the difference - if A and B mismatch, only A will be a key in the map
     */
    public Set<InetAddressAndPort> keyHosts()
    {
        return differences.keySet();
    }

    public HostDifferences get(InetAddressAndPort hostWithDifference)
    {
        return differences.get(hostWithDifference);
    }

    public String toString()
    {
        return "DifferenceHolder{" +
               "differences=" + differences +
               '}';
    }

    public boolean hasDifferenceBetween(InetAddressAndPort node1, InetAddressAndPort node2, Range<Token> range)
    {
        HostDifferences diffsNode1 = differences.get(node1);
        if (diffsNode1 != null && diffsNode1.hasDifferencesFor(node2, range))
            return true;
        HostDifferences diffsNode2 = differences.get(node2);
        if (diffsNode2 != null && diffsNode2.hasDifferencesFor(node1, range))
            return true;
        return false;
    }
}
