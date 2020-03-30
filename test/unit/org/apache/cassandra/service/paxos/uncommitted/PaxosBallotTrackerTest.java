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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosState.MaybePromise.Outcome;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.paxos.PaxosState.MaybePromise.Outcome.REJECT;
import static org.apache.cassandra.service.paxos.PaxosState.ballotTracker;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.PAXOS_CFS;

public class PaxosBallotTrackerTest
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBallotTrackerTest.class);

    protected static String ks;
    protected static final String tbl = "tbl";
    protected static TableMetadata cfm;

    // which stage the ballot is tested at
    enum Stage { PREPARE, PROPOSE, COMMIT }
    enum Order
    {
        FIRST, // first ballot
        SUBSEQUENT, // newest ballot
        SUPERSEDED // ballot
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();

        ks = "coordinatorsessiontest";
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
    }

    @Before
    public void setUp()
    {
        PAXOS_CFS.truncateBlocking();
        PaxosState.unsafeReset();
    }

    private static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    private static void testHighBound(Stage stage, Order order)
    {
        logger.info("testHighBound for {} {} ", stage, order);
        Ballot ballot1 = Paxos.ballotForConsistency(1001, ConsistencyLevel.SERIAL);
        Ballot ballot2 = Paxos.ballotForConsistency(1002, ConsistencyLevel.SERIAL);

        Ballot opBallot;
        Ballot expected;
        switch (order)
        {
            case FIRST:
                opBallot = ballot1;
                expected = ballot1;
                break;
            case SUBSEQUENT:
                ballotTracker().updateHighBoundUnsafe(Ballot.none(), ballot1);
                Assert.assertEquals(ballot1, ballotTracker().getHighBound());
                opBallot = ballot2;
                expected = ballot2;
                break;
            case SUPERSEDED:
                ballotTracker().updateHighBoundUnsafe(Ballot.none(), ballot2);
                Assert.assertEquals(ballot2, ballotTracker().getHighBound());
                opBallot = ballot1;
                expected = ballot2;
                break;
            default:
                throw new AssertionError();
        }

        DecoratedKey key = dk(1);
        Commit.Proposal commit = new Commit.Proposal(opBallot, PaxosRowsTest.nonEmptyUpdate(opBallot, cfm, key));

        switch (stage)
        {
            case PREPARE:
                try (PaxosState state = PaxosState.get(commit))
                {
                    state.promiseIfNewer(commit.ballot, true);
                }
                break;
            case PROPOSE:
                try (PaxosState state = PaxosState.get(commit))
                {
                    state.acceptIfLatest(commit);
                }
                break;
            case COMMIT:
                PaxosState.commitDirect(commit);
                break;
            default:
                throw new AssertionError();
        }

        Assert.assertEquals(expected, ballotTracker().getHighBound());
    }

    /**
     * Tests that the ballot high bound is set correctly for all update types
     */
    @Test
    public void highBound()
    {
        for (Stage stage: Stage.values())
        {
            for (Order order: Order.values())
            {
                setUp();
                testHighBound(stage, order);
            }
        }
    }

    @Test
    public void lowBoundSet() throws IOException
    {
        PaxosBallotTracker ballotTracker = ballotTracker();
        Ballot ballot1 = Paxos.ballotForConsistency(1001, ConsistencyLevel.SERIAL);
        Ballot ballot2 = Paxos.ballotForConsistency(1002, ConsistencyLevel.SERIAL);
        Ballot ballot3 = Paxos.ballotForConsistency(1003, ConsistencyLevel.SERIAL);

        Assert.assertEquals(Ballot.none(), ballotTracker.getLowBound());

        ballotTracker.updateLowBound(ballot2);
        Assert.assertEquals(ballot2, ballotTracker.getLowBound());

        ballotTracker.updateLowBound(ballot1);
        Assert.assertEquals(ballot2, ballotTracker.getLowBound());

        ballotTracker.updateLowBound(ballot3);
        Assert.assertEquals(ballot3, ballotTracker.getLowBound());
    }

    @Test
    public void lowBoundPrepare() throws IOException
    {
        PaxosBallotTracker ballotTracker = ballotTracker();
        Ballot ballot1 = Paxos.ballotForConsistency(1001, ConsistencyLevel.SERIAL);
        Ballot ballot2 = Paxos.ballotForConsistency(1002, ConsistencyLevel.SERIAL);
        Ballot ballot3 = Paxos.ballotForConsistency(1003, ConsistencyLevel.SERIAL);
        Ballot ballot4 = Paxos.ballotForConsistency(1004, ConsistencyLevel.SERIAL);

        ballotTracker.updateLowBound(ballot1);
        Assert.assertNotNull(ballotTracker.getLowBound());

        DecoratedKey key = dk(1);
        try (PaxosState state = PaxosState.get(key, cfm))
        {
            PaxosState.MaybePromise promise = state.promiseIfNewer(ballot2, true);
            Assert.assertEquals(Outcome.PROMISE, promise.outcome());
            Assert.assertNull(promise.supersededBy());
        }

        // set the lower bound into the 'future', and prepare with an earlier ballot
        ballotTracker.updateLowBound(ballot4);
        try (PaxosState state = PaxosState.get(key, cfm))
        {
            PaxosState.MaybePromise promise = state.promiseIfNewer(ballot3, true);
            Assert.assertEquals(REJECT, promise.outcome());
            Assert.assertEquals(ballot4, promise.supersededBy());
        }
    }

    @Test
    public void lowBoundAccept() throws IOException
    {
        PaxosBallotTracker ballotTracker = ballotTracker();
        Ballot ballot1 = Paxos.ballotForConsistency(1001, ConsistencyLevel.SERIAL);
        Ballot ballot2 = Paxos.ballotForConsistency(1002, ConsistencyLevel.SERIAL);
        Ballot ballot3 = Paxos.ballotForConsistency(1003, ConsistencyLevel.SERIAL);
        Ballot ballot4 = Paxos.ballotForConsistency(1004, ConsistencyLevel.SERIAL);

        ballotTracker.updateLowBound(ballot1);
        Assert.assertNotNull(ballotTracker.getLowBound());

        DecoratedKey key = dk(1);
        try (PaxosState state = PaxosState.get(key, cfm))
        {
            Ballot result = state.acceptIfLatest(new Commit.Proposal(ballot2, PartitionUpdate.emptyUpdate(cfm, key)));
            Assert.assertNull(result);
        }

        // set the lower bound into the 'future', and prepare with an earlier ballot
        ballotTracker.updateLowBound(ballot4);
        try (PaxosState state = PaxosState.get(key, cfm))
        {
            Ballot result = state.acceptIfLatest(new Commit.Proposal(ballot3, PartitionUpdate.emptyUpdate(cfm, key)));
            Assert.assertEquals(ballot4, result);
        }
    }

    /**
     * updating the lower bound should persist it to disk
     */
    @Test
    public void persistentLowBound() throws IOException
    {
        PaxosBallotTracker ballotTracker = ballotTracker();
        Ballot ballot1 = Paxos.ballotForConsistency(1001, ConsistencyLevel.SERIAL);
        Assert.assertEquals(Ballot.none(), ballotTracker.getLowBound());

        // a new tracker shouldn't load a ballot
        PaxosBallotTracker tracker2 = PaxosBallotTracker.load(ballotTracker.getDirectory());
        Assert.assertEquals(Ballot.none(), tracker2.getLowBound());

        // updating the lower bound should flush it to disk
        ballotTracker.updateLowBound(ballot1);
        Assert.assertEquals(ballot1, ballotTracker.getLowBound());

        // then loading a new tracker should find the lower bound
        PaxosBallotTracker tracker3 = PaxosBallotTracker.load(ballotTracker.getDirectory());
        Assert.assertEquals(ballot1, tracker3.getLowBound());
    }
}
