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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Strategy that replicate data on every {@code live} node.
 *
 * <p>This strategy is a {@code MultiDatacentersStrategy}. By consequence, it will handle properly local consistency levels.
 * Nevertheless, as the data is replicated on every node, consistency levels such as QUORUM should not be used
 * on clusters having more than 5 nodes.<p>
 *
 * <p>During bootstrap the time at which the data will be available is unknown and if the bootstrap is performed with
 * autobootstrap=false on a seed node, there will be no data locally until rebuild is run.</p>
 *
 */
public class EverywhereStrategy extends AbstractReplicationStrategy
{
    public EverywhereStrategy(String keyspaceName,
                              TokenMetadata tokenMetadata,
                              IEndpointSnitch snitch,
                              Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token searchToken, TokenMetadata tokenMetadata)
    {
        // Even if primary range repairs do not make a lot of sense for this strategy we want the behavior to be
        // correct if somebody use it.
        // Primary range repair expect the first endpoint of the list to be the primary range owner.
        Set<Replica> replicas = new LinkedHashSet<>();
        Iterator<Token> iter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);

        if (iter.hasNext())
        {
            Token end = iter.next();
            Token start = tokenMetadata.getPredecessor(end);
            Range<Token> range = new Range<>(start, end);

            InetAddressAndPort endpoint = tokenMetadata.getEndpoint(end);
            replicas.add(Replica.fullReplica(endpoint, range));

            while (iter.hasNext())
            {
                endpoint = tokenMetadata.getEndpoint(iter.next());
                replicas.add(Replica.fullReplica(endpoint, range));
            }
        }

        return EndpointsForRange.copyOf(replicas);
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return ReplicationFactor.fullOnly(getSizeOfRingMemebers());
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        // noop
    }

    @Override
    public void maybeWarnOnOptions()
    {
        // noop
    }

    @Override
    public Collection<String> recognizedOptions()
    {
        return Collections.emptyList();
    }

    /**
     * CASSANDRA-12510 added a check that forbids decommission when the number of
     * nodes will drop below the RF for a given keyspace. This check is breaking on
     * EverywhereStrategy because all nodes replicate the keyspace, so this check does
     * not make sense for partitioned keyspaces such as LocalStrategy and EverywhereStrategy.
     *
     * @return <code>false</code> because the data is not partitioned across the ring.
     */
    @Override
    public boolean isPartitioned()
    {
        return false;
    }
}
