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
package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.DistributedTestSnitch;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipAccessUtils;
import org.apache.cassandra.gms.Gossiper;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.config.CassandraRelevantProperties.HOST_REPLACE_TOKENS;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOOSE_DEF_OF_EMPTY_ENABLED;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.gms.GossipAccessUtils.forceNewerGenerationUnsafe;

/**
 * There are cases where -Dcassandra.allow_empty_replace_address=true does not work, but the system submitting
 * the host replacement do in fact know the original tokens / host_id, so can leverage this information to allow
 * the replacement to continue.
 */
public class ReplaceHostWithTokensAndIDTest extends TestBaseImpl
{
    @Test
    public void withoutStatus() throws IOException
    {
        test((cluster, seed, nodeToRemove, tokens) -> {
            removeApplicationStates(seed, nodeToRemove,
                                    ApplicationState.STATUS, ApplicationState.STATUS_WITH_PORT,
                                    ApplicationState.TOKENS, ApplicationState.HOST_ID);

            // Without this patch this test would fail with "Cannot replace token X which does not exist!"
            return replaceHostAndStart(cluster, nodeToRemove, p -> {
                // Since node2 was killed abruptly it's possible that node2's gossip state has an old schema version.
                // If this happens then bootstrap will fail waiting for a schema version it will never see; to avoid
                // this, setting this property to log the warning rather than fail bootstrap.
                p.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                p.set(LOOSE_DEF_OF_EMPTY_ENABLED, true);
                p.set(HOST_REPLACE_TOKENS, tokens);
            });
        });
    }

    @Test
    public void withStatus() throws IOException
    {
        test((cluster, seed, nodeToRemove, tokens) -> {
            removeApplicationStates(seed, nodeToRemove, ApplicationState.TOKENS);

            // Without this patch this test would fail with "Cannot replace token X which does not exist!"
            return replaceHostAndStart(cluster, nodeToRemove, p -> {
                // Since node2 was killed abruptly it's possible that node2's gossip state has an old schema version.
                // If this happens then bootstrap will fail waiting for a schema version it will never see; to avoid
                // this, setting this property to log the warning rather than fail bootstrap.
                p.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                p.set(HOST_REPLACE_TOKENS, tokens);
            });
        });
    }

    private interface TestCase
    {
        IInvokableInstance apply(Cluster cluster, IInvokableInstance seed, IInvokableInstance nodeToRemove, List<String> tokens);
    }

    private void test(TestCase test) throws IOException
    {
        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .withTokenSupplier(n -> ts.token(n == 3 ? 2 : n))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            List<String> tokens = getTokens(nodeToRemove);

            setupCluster(cluster, seed);

            // remove the node and corrupt gossip (remove status)
            stopUnchecked(nodeToRemove);
            IInvokableInstance replacingNode = test.apply(cluster, seed, nodeToRemove, tokens);

            awaitRingJoin(seed, replacingNode);

            // confirm read/write still works
            seed.coordinator().execute(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), ConsistencyLevel.ALL, 1);
            SimpleQueryResult results = replacingNode.executeInternalWithResult(withKeyspace("SELECT * FROM %s.tbl"));
            AssertUtils.assertRows(results, QueryResults.builder()
                                                        .columns("pk")
                                                        .row(42)
                                                        .row(1)
                                                        .build());
        }
    }

    private static void removeApplicationStates(IInvokableInstance seed, IInvokableInstance nodeToRemove, ApplicationState... states)
    {
        InetSocketAddress addressToRemove = nodeToRemove.config().broadcastAddress();
        seed.runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(DistributedTestSnitch.toCassandraInetAddressAndPort(addressToRemove));
            GossipAccessUtils.removeApplicationStates(state, states);
            forceNewerGenerationUnsafe(state);
        }));
    }

    private void setupCluster(Cluster cluster, IInvokableInstance seed)
    {
        fixDistributedSchemas(cluster);
        init(cluster);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk INT PRIMARY KEY);"));
        seed.coordinator().execute(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), ConsistencyLevel.ALL, 42);
    }

    private List<String> getTokens(IInvokableInstance nodeToRemove)
    {
        return nodeToRemove.callOnInstance(() -> SystemKeyspace.getSavedTokens().stream().map(Object::toString).collect(Collectors.toList()));
    }
}