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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.MAX_LOCAL_PAUSE_IN_MS;
import static org.junit.Assert.assertFalse;

public class FailureDetectorTest
{
    @BeforeClass
    public static void setup()
    {
        // slow unit tests can cause problems with FailureDetector's GC pause handling
        MAX_LOCAL_PAUSE_IN_MS.setLong(20000);

        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void testConvictAfterLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        // we want to convict if there is any heartbeat data present in the FD
        DatabaseDescriptor.setPhiConvictThreshold(0);

        // create a ring of 2 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 3);

        InetAddressAndPort leftHost = hosts.get(1);

        FailureDetector.instance.report(leftHost);

        // trigger handleStateLeft in StorageService
        ss.onChange(leftHost, ApplicationState.STATUS_WITH_PORT,
                    valueFactory.left(Collections.singleton(endpointTokens.get(1)), Gossiper.computeExpireTime()));

        // confirm that handleStateLeft was called and leftEndpoint was removed from TokenMetadata
        assertFalse("Left endpoint not removed from TokenMetadata", tmd.isMember(leftHost));

        // confirm the FD's history for leftHost didn't get wiped by status jump to LEFT
        FailureDetector.instance.interpret(leftHost);
        assertFalse("Left endpoint not convicted", FailureDetector.instance.isAlive(leftHost));
    }
}
