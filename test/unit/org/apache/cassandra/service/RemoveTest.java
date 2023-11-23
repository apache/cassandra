/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.tcm.membership.MembershipUtils.*;

public class RemoveTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    static final IPartitioner partitioner = RandomPartitioner.instance;
    StorageService ss = StorageService.instance;
    static IPartitioner oldPartitioner;
    ArrayList<Token> endpointTokens = new ArrayList<Token>();
    ArrayList<Token> keyTokens = new ArrayList<Token>();
    List<InetAddressAndPort> hosts = new ArrayList<>();
    List<UUID> hostIds = new ArrayList<UUID>();
    InetAddressAndPort removalhost;
    UUID removalId;

    @BeforeClass
    public static void setupClass() throws ConfigurationException
    {
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        ServerTestUtils.prepareServerNoRegister();
        MessagingService.instance().listen();
    }

    @AfterClass
    public static void tearDownClass()
    {
        StorageService.instance.setPartitionerUnsafe(oldPartitioner);
    }

    @Before
    public void setup() throws IOException, ConfigurationException
    {
        ServerTestUtils.resetCMS();

        // create a ring of 5 nodes
        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, 6);

        removalhost = hosts.get(5);
        hosts.remove(removalhost);
        removalId = hostIds.get(5);
        hostIds.remove(removalId);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBadHostId()
    {
        ss.removeNode("ffffffff-aaaa-aaaa-aaaa-ffffffffffff");

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalHostId()
    {
        //first ID should be localhost
        ss.removeNode(hostIds.get(0).toString());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNonmemberId()
    {
        VersionedValueFactory valueFactory = new VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        InetAddressAndPort joiningEndpoint = endpoint(99);
        NodeId joiningId = ClusterMetadataTestHelper.register(joiningEndpoint);
        ClusterMetadataTestHelper.joinPartially(joiningEndpoint, DatabaseDescriptor.getPartitioner().getRandomToken());
        ss.removeNode(joiningId.toUUID().toString());
    }
}
