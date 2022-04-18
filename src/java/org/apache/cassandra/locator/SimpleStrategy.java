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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;


/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the RF nodes that lie right next to each other
 * on the ring.
 */
public class SimpleStrategy extends AbstractReplicationStrategy
{
    private static final String REPLICATION_FACTOR = "replication_factor";
    private static final Logger logger = LoggerFactory.getLogger(SimpleStrategy.class);
    private final ReplicationFactor rf;

    public SimpleStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
        validateOptionsInternal(configOptions);
        this.rf = ReplicationFactor.fromString(this.configOptions.get(REPLICATION_FACTOR));
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token token, TokenMetadata metadata)
    {
        ArrayList<Token> ring = metadata.sortedTokens();
        if (ring.isEmpty())
            return EndpointsForRange.empty(new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getMinimumToken()));

        Token replicaEnd = TokenMetadata.firstToken(ring, token);
        Token replicaStart = metadata.getPredecessor(replicaEnd);
        Range<Token> replicaRange = new Range<>(replicaStart, replicaEnd);
        Iterator<Token> iter = TokenMetadata.ringIterator(ring, token, false);

        EndpointsForRange.Builder replicas = new EndpointsForRange.Builder(replicaRange, rf.allReplicas);

        // Add the token at the index by default
        while (replicas.size() < rf.allReplicas && iter.hasNext())
        {
            Token tk = iter.next();
            InetAddressAndPort ep = metadata.getEndpoint(tk);
            if (!replicas.endpoints().contains(ep))
                replicas.add(new Replica(ep, replicaRange, replicas.size() < rf.fullReplicas));
        }

        return replicas.build();
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return rf;
    }

    private static void validateOptionsInternal(Map<String, String> configOptions) throws ConfigurationException
    {
        if (configOptions.get(REPLICATION_FACTOR) == null)
            throw new ConfigurationException("SimpleStrategy requires a replication_factor strategy option.");
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        validateOptionsInternal(configOptions);
        validateReplicationFactor(configOptions.get(REPLICATION_FACTOR));
    }

    @Override
    public void maybeWarnOnOptions(ClientState state)
    {
        if (!SchemaConstants.isSystemKeyspace(keyspaceName))
        {
            int nodeCount = StorageService.instance.getHostIdToEndpoint().size();
            // nodeCount==0 on many tests
            Guardrails.minimumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
            Guardrails.maximumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
            if (rf.fullReplicas > nodeCount && nodeCount != 0)
            {
                String msg = "Your replication factor " + rf.fullReplicas
                             + " for keyspace "
                             + keyspaceName
                             + " is higher than the number of nodes "
                             + nodeCount;
                ClientWarn.instance.warn(msg);
                logger.warn(msg);
            }
        }
    }

    @Override
    public Collection<String> recognizedOptions()
    {
        return Collections.singleton(REPLICATION_FACTOR);
    }

    protected static void prepareOptions(Map<String, String> options, Map<String, String> previousOptions)
    {
        // When altering from NTS to SS, previousOptions could have multiple different RFs for different data centers - so we
        // will instead default to DefaultRF configuration if RF is not mentioned with the alter statement
        String rf = previousOptions.containsKey(REPLICATION_FACTOR) ? previousOptions.get(REPLICATION_FACTOR)
                                                                    : Integer.toString(DatabaseDescriptor.getDefaultKeyspaceRF());
        options.putIfAbsent(REPLICATION_FACTOR, rf);
    }
}
