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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(BMUnitRunner.class)
@BMRule(name = "FailureDecector sees all nodes as down", // applies to all test cases in the class
targetClass = "FailureDetector",
targetMethod = "isAlive",
action = "return false;")
public class AssureSufficientLiveNodesAllNodesDownTest
{
    private static final AtomicInteger testIdGen = new AtomicInteger(0);
    private static final Supplier<String> keyspaceNameGen = () -> "race_" + testIdGen.getAndIncrement();
    private static final Token tk = new Murmur3Partitioner.LongToken(0);

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.addressBytes;
                return "rake" + address[1];
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.addressBytes;
                return "datacenter" + address[1];
            }
        });

        List<InetAddressAndPort> instances = ImmutableList.of(
        // datacenter 1
        InetAddressAndPort.getByName("127.1.0.255"), InetAddressAndPort.getByName("127.1.0.254"), InetAddressAndPort.getByName("127.1.0.253"),
        // datacenter 2
        InetAddressAndPort.getByName("127.2.0.255"), InetAddressAndPort.getByName("127.2.0.254"), InetAddressAndPort.getByName("127.2.0.253"),
        // datacenter 3
        InetAddressAndPort.getByName("127.3.0.255"), InetAddressAndPort.getByName("127.3.0.254"), InetAddressAndPort.getByName("127.3.0.253"));

        for (int i = 0; i < instances.size(); i++)
        {
            InetAddressAndPort ip = instances.get(i);
            metadata.updateHostId(UUID.randomUUID(), ip);
            metadata.updateNormalToken(new Murmur3Partitioner.LongToken(i), ip);
        }
    }

    @Test
    public void insufficientLiveNodesForWriteTest()
    {
        final KeyspaceParams rf = KeyspaceParams.nts("datacenter1", 3);
        Keyspace keyspace = getKeyspace(rf);
        assertThatThrownBy(() ->
                           ReplicaPlans.forWrite(keyspace, QUORUM, tk, ReplicaPlans.writeNormal)
        ).as("Unavailable should be thrown given 0 live nodes is less than a quorum of 3")
         .isInstanceOf(UnavailableException.class)
         .hasMessageContaining("Cannot achieve consistency level QUORUM");
    }

    @Test
    public void noCheckForLiveNodesForViewWriteTest()
    {
        // test for materialized view, there should be no sufficient live nodes check
        final KeyspaceParams rf = KeyspaceParams.nts("datacenter1", 3);
        Keyspace keyspace = getKeyspace(rf);
        ReplicaPlans.forViewWrite(keyspace, ONE, ReplicaLayout.forTokenWriteLiveAndDown(keyspace, tk), ReplicaPlans.writeAll);
    }

    private static Keyspace getKeyspace(KeyspaceParams kp)
    {
        String keyspaceName = keyspaceNameGen.get();
        KeyspaceMetadata initKsMeta = KeyspaceMetadata.create(keyspaceName, kp, Tables.of(SchemaLoader.standardCFMD(keyspaceName, "Bar").build()));
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(initKsMeta));
        return Keyspace.open(keyspaceName);
    }
}
