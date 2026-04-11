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

package org.apache.cassandra.distributed.test.ring;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.config.CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class BootstrapResetProgressTest extends TestBaseImpl
{
    /**
     * Confirm that, in the absence of the reset_bootstrap_progress param being set and in the face of a found prior
     * partial bootstrap, we error out and don't complete our bootstrap.
     *
     * Test w/out vnodes only; logic is identical for both run env but the token alloc in this test doesn't work for
     * vnode env and it's not worth the lift to update it to work in both env.
     *
     * @throws Throwable
     */
    @Test
    public void bootstrapUnspecifiedFailsOnResumeTest() throws Throwable
    {
        RESET_BOOTSTRAP_PROGRESS.reset();

        // Need our partitioner active for rangeToBytes conversion below
        Config c = DatabaseDescriptor.loadConfig();
        DatabaseDescriptor.daemonInitialization(() -> c);

        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        boolean sawException = false;
        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withoutVNodes()
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            BootstrapTest.populate(cluster, 0, 100);

            IInstanceConfig config = cluster.newInstanceConfig().set("auto_bootstrap", "true");
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.runOnInstance(() -> {
                JOIN_RING.setBoolean(false);
            });
            newInstance.startup();

            List<Token> tokens = cluster.tokens();
            assert tokens.size() >= 3;

            /*
            Our local tokens:
            Tokens in cluster tokens: [-3074457345618258603, 3074457345618258601, 9223372036854775805]

            From the bootstrap process:
            fetchReplicas in our test keyspace:
            [FetchReplica
                {local=Full(/127.0.0.3:7012,(-3074457345618258603,3074457345618258601]),
                remote=Full(/127.0.0.1:7012,(-3074457345618258603,3074457345618258601])},
            FetchReplica
                {local=Full(/127.0.0.3:7012,(9223372036854775805,-3074457345618258603]),
                remote=Full(/127.0.0.1:7012,(9223372036854775805,-3074457345618258603])},
            FetchReplica
                {local=Full(/127.0.0.3:7012,(3074457345618258601,9223372036854775805]),
                remote=Full(/127.0.0.1:7012,(3074457345618258601,9223372036854775805])}]
             */

            // Insert some bogus ranges in the keyspace to be bootstrapped to trigger the check on available ranges on bootstrap.
            // Note: these have to precisely overlap with the token ranges hit during streaming or they won't trigger the
            // availability logic on bootstrap to then except out; we can't just have _any_ range for a keyspace, but rather,
            // must have a range that overlaps with what we're trying to stream.
            Set<Range<Token>> fullSet = new HashSet<>();
            fullSet.add(new Range<>(tokens.get(0), tokens.get(1)));
            fullSet.add(new Range<>(tokens.get(1), tokens.get(2)));
            fullSet.add(new Range<>(tokens.get(2), tokens.get(0)));

            // Should be fine to trigger on full ranges only but add a partial for good measure.
            Set<Range <Token>> partialSet = new HashSet<>();
            partialSet.add(new Range<>(tokens.get(2), tokens.get(1)));

            String cql = String.format("INSERT INTO %s.%s (keyspace_name, full_ranges, transient_ranges) VALUES (?, ?, ?)",
                                       SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                       SystemKeyspace.AVAILABLE_RANGES_V2);

            newInstance.executeInternal(cql,
                                        KEYSPACE,
                                       fullSet.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet()),
                                        partialSet.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet()));

            newInstance.runOnInstance(() -> {
                try
                {
                    StorageService.instance.joinRing();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (AssumptionViolatedException ave)
        {
            // We get an AssumptionViolatedException if we're in a test job configured w/vnodes
            throw ave;
        }
        catch (RuntimeException rte)
        {
            if (rte.getMessage().contains("Discovered existing bootstrap data"))
                sawException = true;
        }
        Assert.assertTrue("Expected to see a RuntimeException w/'Discovered existing bootstrap data' in the error message; did not.",
                          sawException);
    }
}
