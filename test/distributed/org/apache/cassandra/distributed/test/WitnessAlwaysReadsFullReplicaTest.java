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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.locator.BaseProximity;
import org.apache.cassandra.locator.Endpoint;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.utils.Sortable;

/**
 * Make sure that if the fastest available replicas are transient that we don't hit an error
 * by forgetting to contact a full replica.
 */
@RunWith(Parameterized.class)
public class WitnessAlwaysReadsFullReplicaTest extends SharedClusterTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(WitnessAlwaysReadsFullReplicaTest.class);

    /**
     * The consistency level to test.
     */
    @Parameterized.Parameter
    public ConsistencyLevel consistencyLevel;

    private static String table = "test_tbl";
    private static String qualifiedTable = KEYSPACE + "." + table;

    @Parameterized.Parameters(name = "CL={0}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        // Test all consistency levels except NODE_LOCAL as requested
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            if (cl == ConsistencyLevel.ANY
                || cl == ConsistencyLevel.NODE_LOCAL
                || cl == ConsistencyLevel.SERIAL
                || cl == ConsistencyLevel.LOCAL_SERIAL)
                continue;
            result.add(new Object[]{ cl });
        }
        return result;
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        // Set up shared cluster - for DC-aware consistency levels, we need multi-DC setup
        setupCluster(6, builder -> builder.withRacks(2, 1, 3)  // 2 DCs with 3 nodes each
                                          .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                .with(Feature.GOSSIP)
                                                                .set("transient_replication_enabled", "true")
                                                                .set("dynamic_snitch", false)  // Disable dynamic snitch
                                                                .set("node_proximity", TransientFirstProximity.class.getName()))); // Use our custom proximity
        
        // Create keyspace and table for the entire test suite
        createKeyspace("{'class': 'NetworkTopologyStrategy', 'datacenter1': '3/1', 'datacenter2': '3/1'} AND replication_type='tracked'");
        createTable("CREATE TABLE " + qualifiedTable + " (k int primary key, v int)");
    }

    @Override
    protected void truncateTables()
    {
        // Truncate the test table after each test
        SHARED_CLUSTER.schemaChange("TRUNCATE " + qualifiedTable);
    }

    /**
     * Custom proximity implementation that prioritizes transient replicas first.
     * This ensures that contactForRead will select transient replicas before full replicas,
     * forcing the bug scenario where only transient replicas would be contacted.
     */
    public static class TransientFirstProximity extends BaseProximity
    {
        @Override
        public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedReplicas)
        {
            // Sort replicas to put transient replicas first, then full replicas
            return unsortedReplicas.sorted((r1, r2) ->
            {
                // Transient replicas come first (lower value = higher priority)
                if (r1.isTransient() && !r2.isTransient()) return -1;  // r1 (transient) comes before r2 (full)
                if (!r1.isTransient() && r2.isTransient()) return 1;   // r2 (transient) comes before r1 (full)
                return 0; // Same type, maintain stable order
            });
        }

        @Override
        public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
        {
            // Transient replicas come first (lower value = higher priority)
            if (r1.isTransient() && !r2.isTransient()) return -1;  // r1 (transient) comes before r2 (full)
            if (!r1.isTransient() && r2.isTransient()) return 1;   // r2 (transient) comes before r1 (full)
            return 0; // Same type, maintain stable order
        }

        @Override
        public boolean supportCompareByEndpoint()
        {
            return true;
        }

        @Override
        public <C extends Sortable<? extends Endpoint, ? extends C>> Comparator<Endpoint> endpointComparator(InetAddressAndPort address, C addresses)
        {
            return this::compareByEndpoint;
        }

        private int compareByEndpoint(Endpoint a, Endpoint b)
        {
            // For endpoints that are replicas, prioritize transient replicas first
            if (a instanceof Replica && b instanceof Replica)
            {
                Replica r1 = (Replica) a;
                Replica r2 = (Replica) b;
                if (r1.isTransient() && !r2.isTransient()) return -1;  // r1 (transient) comes before r2 (full)
                if (!r1.isTransient() && r2.isTransient()) return 1;   // r2 (transient) comes before r1 (full)
            }
            return 0; // Same type or not replicas, maintain stable order
        }
    }

    @Test
    public void testContactForReadBugCausesUnavailableException() throws Throwable
    {
        try
        {
            // This read should fail with UnavailableException because our custom proximity
            // ensures transient replicas are sorted first, so contactForRead selects only transient replicas
            SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + qualifiedTable + " WHERE k = 1", consistencyLevel);
        }
        catch (Exception e)
        {
            throw e;
        }
    }
}
