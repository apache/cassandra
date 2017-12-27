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

package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GossiperTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("schema_test_ks",
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD("schema_test_ks", "schema_test_cf"));
    }

    static final IPartitioner partitioner = new RandomPartitioner();
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = StorageService.instance.getTokenMetadata();
    ArrayList<Token> endpointTokens = new ArrayList<>();
    ArrayList<Token> keyTokens = new ArrayList<>();
    List<InetAddress> hosts = new ArrayList<>();
    List<UUID> hostIds = new ArrayList<>();

    @Before
    public void setup()
    {
        tmd.clearUnsafe();
    }

    @Test
    public void testLargeGenerationJump() throws UnknownHostException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        InetAddress remoteHostAddress = hosts.get(1);

        EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
        HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

        //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
        assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

        HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration() + Gossiper.MAX_GENERATION_DIFFERENCE + 1);
        EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

        //The generation should have been updated because it isn't over Gossiper.MAX_GENERATION_DIFFERENCE in the future
        HeartBeatState actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
        assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());

        //Propose a generation 10 years in the future - this should be rejected.
        HeartBeatState badProposedRemoteHeartBeat = new HeartBeatState((int) (System.currentTimeMillis()/1000) + Gossiper.MAX_GENERATION_DIFFERENCE * 10);
        EndpointState badProposedRemoteState = new EndpointState(badProposedRemoteHeartBeat);

        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, badProposedRemoteState));

        actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();

        //The generation should not have been updated because it is over Gossiper.MAX_GENERATION_DIFFERENCE in the future
        assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());
    }

    @Test
    public void testSchemaVersionUpdate() throws UnknownHostException, InterruptedException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        MessagingService.instance().listen();
        Gossiper.instance.start(1);
        InetAddress remoteHostAddress = hosts.get(1);

        EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
        // Set to any 3.0 version
        Gossiper.instance.injectApplicationState(remoteHostAddress, ApplicationState.RELEASE_VERSION, StorageService.instance.valueFactory.releaseVersion("3.0.14"));

        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, initialRemoteState));

        // wait until the schema is set
        VersionedValue schema = null;
        for (int i = 0; i < 10; i++)
        {
            EndpointState localState = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(0));
            schema = localState.getApplicationState(ApplicationState.SCHEMA);
            if (schema != null)
                break;
            Thread.sleep(1000);
        }

        // schema is set and equals to "alternative" version
        assertTrue(schema != null);
        assertEquals(schema.value, Schema.instance.getAltVersion().toString());

        // Upgrade remote host version to the latest one (3.11)
        Gossiper.instance.injectApplicationState(remoteHostAddress, ApplicationState.RELEASE_VERSION, StorageService.instance.valueFactory.releaseVersion());

        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, initialRemoteState));

        // wait until the schema change
        VersionedValue newSchema = null;
        for (int i = 0; i < 10; i++)
        {
            EndpointState localState = Gossiper.instance.getEndpointStateForEndpoint(hosts.get(0));
            newSchema = localState.getApplicationState(ApplicationState.SCHEMA);
            if (!schema.value.equals(newSchema.value))
                break;
            Thread.sleep(1000);
        }

        // schema is changed and equals to real version
        assertFalse(schema.value.equals(newSchema.value));
        assertEquals(newSchema.value, Schema.instance.getRealVersion().toString());
    }
}
