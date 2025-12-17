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

package org.apache.cassandra.tcm.transformations;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.OwnershipUtils;

import static org.apache.cassandra.tcm.membership.MembershipUtils.uniqueEndpoints;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrepareJoinTest
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareJoinTest.class);
    private Random random;
    private IPartitioner partitioner;
    private NodeId other;
    private NodeId joining;

    @Before
    public void setup()
    {
        long seed = System.nanoTime();
        logger.info("Running test with seed {}", seed);
        random = new Random(seed);
        partitioner = Murmur3Partitioner.instance;
        other = new NodeId(1);
        joining = new NodeId(2);
    }

    @Test
    public void singletonTokenAlreadyAssigned()
    {
        ClusterMetadata metadata = metadata();
        Collection<Token> assigned = metadata.tokenMap.tokens(other);
        Set<Token> toJoin = Collections.singleton(assigned.iterator().next());

        PrepareJoin prepare = new PrepareJoin(joining, toJoin, PrepareLeaveTest.dummyPlacementProvider, true, true);
        Transformation.Result result = prepare.execute(metadata);
        assertTrue(result.isRejected());
        assertEquals(ExceptionCode.INVALID, result.rejected().code);
        assertTrue(result.rejected().reason.startsWith("Rejecting this plan as some tokens are already assigned"));
    }

    @Test
    public void multipleTokensAlreadyAssigned()
    {
        ClusterMetadata metadata = metadata();
        Collection<Token> assigned = metadata.tokenMap.tokens(other);
        Set<Token> toJoin = new HashSet<>(assigned);

        PrepareJoin prepare = new PrepareJoin(joining, toJoin, PrepareLeaveTest.dummyPlacementProvider, true, true);
        Transformation.Result result = prepare.execute(metadata);
        assertTrue(result.isRejected());
        assertEquals(ExceptionCode.INVALID, result.rejected().code);
        assertTrue(result.rejected().reason.startsWith("Rejecting this plan as some tokens are already assigned"));
    }

    @Test
    public void noTokensAlreadyAssigned()
    {
        ClusterMetadata metadata = metadata();
        Collection<Token> assigned = metadata.tokenMap.tokens(other);
        Set<Token> toJoin = OwnershipUtils.randomTokens(16, partitioner, random);
        while (!Sets.intersection(new HashSet<>(assigned), toJoin).isEmpty())
            toJoin = OwnershipUtils.randomTokens(16, partitioner, random);

        PrepareJoin prepare = new PrepareJoin(joining, toJoin, PrepareLeaveTest.dummyPlacementProvider, true, true);
        Transformation.Result result = prepare.execute(metadata);
        assertTrue(result.isSuccess());
    }

    private ClusterMetadata metadata()
    {
        partitioner = Murmur3Partitioner.instance;
        other = new NodeId(1);
        joining = new NodeId(2);
        Location location = new Location("dc", "rack");
        Iterator<InetAddressAndPort> endpoints = uniqueEndpoints(random, 2).iterator();
        Directory directory = new Directory().unsafeWithNodeForTesting(other, new NodeAddresses(endpoints.next()), location, NodeVersion.CURRENT)
                                             .withNodeState(other, NodeState.JOINED)
                                             .unsafeWithNodeForTesting(joining, new NodeAddresses(endpoints.next()), location, NodeVersion.CURRENT)
                                             .withNodeState(joining, NodeState.REGISTERED);
        Set<Token> ownedTokens = OwnershipUtils.randomTokens(16, partitioner, random);
        return ClusterMetadataTestHelper.minimalForTesting(partitioner)
                                        .transformer()
                                        .with(directory)
                                        .proposeToken(other, ownedTokens)
                                        .build().metadata;
    }
}
