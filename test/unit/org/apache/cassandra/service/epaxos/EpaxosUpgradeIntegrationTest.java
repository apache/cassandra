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

package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.UUIDGen;

public class EpaxosUpgradeIntegrationTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    private static final int KEY_VAL = 50;
    private static final ByteBuffer KEY = key(KEY_VAL);
    private static final Token TOKEN = token(KEY_VAL);

    public static class InstrumentedUpgradeService extends UpgradeService
    {
        public InstrumentedUpgradeService(EpaxosService service, String upgradeTable, String paxosTable)
        {
            super(service, upgradeTable, paxosTable);
        }

        int beginCalls = 0;
        Runnable preBeginHook = null;
        Runnable postBeginHook = null;

        @Override
        protected boolean begin(Range<Token> range, UUID cfId, Scope scope, Collection<InetAddress> endpoints, UUID ballot) throws UpgradeFailure
        {
            try
            {
                if (preBeginHook != null) preBeginHook.run();
                return super.begin(range, cfId, scope, endpoints, ballot);
            }
            finally
            {
                if (postBeginHook != null) postBeginHook.run();
                beginCalls++;
            }
        }

        int upgradeCalls = 0;
        Runnable preUpgradeHook = null;
        Runnable postUpgradeHook = null;

        @Override
        protected void upgrade(Range<Token> range, UUID cfId, Scope scope, Collection<InetAddress> endpoints, UUID ballot) throws UpgradeFailure
        {
            try
            {
                if (preUpgradeHook != null) preUpgradeHook.run();
                super.upgrade(range, cfId, scope, endpoints, ballot);
            }
            finally
            {
                if (postUpgradeHook != null) postUpgradeHook.run();
                upgradeCalls++;
            }
        }

        int completeCalls = 0;
        Runnable preCompleteHook = null;
        Runnable postCompleteHook = null;

        @Override
        protected void complete(Range<Token> range, UUID cfId, Scope scope, Collection<InetAddress> endpoints, UUID ballot)
        {
            try
            {
                if (preCompleteHook != null) preCompleteHook.run();
                super.complete(range, cfId, scope, endpoints, ballot);
            }
            finally
            {
                if (postCompleteHook != null) postCompleteHook.run();
                completeCalls++;
            }
        }

        @Override
        protected void reportUpgradeStatus()
        {
            // no-op
        }
    }
    public static class UpgradeNode extends Node.SingleThreaded
    {

        public final InstrumentedUpgradeService upgradeService;

        public UpgradeNode(int number, Messenger messenger, String dc, String ksName)
        {
            super(number, messenger, dc, ksName);

            upgradeService = new InstrumentedUpgradeService(this, nUpgradeTable(number), nPaxosTable(number));
            for (Map.Entry<MessagingService.Verb, IVerbHandler> entry : verbHandlerMap.entrySet())
            {
                verbHandlerMap.put(entry.getKey(), upgradeService.maybeWrapVerbHandler(entry.getValue()));
            }
            verbHandlerMap.put(MessagingService.Verb.PAXOS_UPGRADE, upgradeService.getVerbHandler());
        }

        public final Set<InetAddress> downNodes = new HashSet<>();
        @Override
        protected Predicate<InetAddress> livePredicate()
        {
            return new Predicate<InetAddress>()
            {
                @Override
                public boolean apply(InetAddress address)
                {
                    return !downNodes.contains(address);
                }
            };
        }

        // since the execution of instances sent via commit is skipped,
        // we check the order of the recordExecuted call instead
        List<UUID> recordedExecutions = new LinkedList<>();

        @Override
        public void recordExecuted(Instance instance, ReplayPosition position, long maxTimestamp)
        {
            recordedExecutions.add(instance.getId());
            super.recordExecuted(instance, position, maxTimestamp);
        }
    }

    @Override
    public Node createNode(int nodeNumber, Messenger messenger, String dc, String ks)
    {
        return new UpgradeNode(nodeNumber, messenger, dc, ks);
    }

    private List<UpgradeNode> unodes;

    @Before
    public void setupUpgrade()
    {
        assert nodes.size() == getReplicationFactor();
        unodes = new ArrayList<>(nodes.size());
        for (Node node: nodes)
        {
            unodes.add((UpgradeNode) node);
            ((MockTokenStateManager) node.getTokenStateManager(Scope.GLOBAL)).setTokens(TOKEN0, TOKEN100);
            ((MockTokenStateManager) node.getTokenStateManager(Scope.LOCAL)).setTokens(TOKEN0, TOKEN100);
        }

        for (UpgradeNode node: unodes)
        {
            node.upgradeService.start();
        }
    }

    @Test
    public void successCase()
    {
        for (UpgradeNode node: unodes)
        {
            Assert.assertFalse(node.upgradeService.isUpgraded());
            Assert.assertFalse(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }

        UpgradeService upgradeService = unodes.get(0).upgradeService;
        upgradeService.upgradeNode();

        for (UpgradeNode node: unodes)
        {
            Assert.assertTrue(node.upgradeService.isUpgraded());
            Assert.assertTrue(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }

        // check that trying again does nothing
        int messagesSent = messenger.getMessagesSent();
        assert messagesSent > 0;
        upgradeService.upgradeNode();
        Assert.assertEquals(messagesSent, messenger.getMessagesSent());
    }

    /**
     * Nothing should happen if one of the nodes is reported down by the failure detector
     */
    @Test
    public void failureDetected()
    {
        for (UpgradeNode node: unodes)
        {
            Assert.assertFalse(node.upgradeService.isUpgraded());
            Assert.assertFalse(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }

        Assert.assertEquals(0, messenger.getMessagesSent());
        UpgradeNode unode = unodes.get(0);
        unode.downNodes.add(unodes.get(1).getEndpoint());

        unode.upgradeService.upgradeNode();

        Assert.assertEquals(0, messenger.getMessagesSent());

        for (UpgradeNode node: unodes)
        {
            Assert.assertFalse(node.upgradeService.isUpgraded());
            Assert.assertFalse(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }
    }

    @Test
    public void minorityMissesCompleteMessage() throws Exception
    {
        for (UpgradeNode node: unodes)
        {
            Assert.assertFalse(node.upgradeService.isUpgraded());
            Assert.assertFalse(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }

        final List<UUID> expectedOrder = new ArrayList<>();
        UpgradeNode unode = unodes.get(0);

        // commit some paxos commits right before sending the complete message
        unode.upgradeService.postUpgradeHook = new Runnable()
        {
            @Override
            public void run()
            {
                // do some commits
                // node 1 gets 2 commits, node 2&3 only gets one
                Commit commit;
                commit = Commit.newPrepare(KEY, cfm, UUIDGen.getTimeUUID());
                InetAddress leader = unodes.get(0).getEndpoint();
                Set<UUID> proposalDeps = new HashSet<>();
                for (UpgradeNode node: unodes)
                {
                    proposalDeps.addAll(node.upgradeService.reportPaxosProposal(commit, leader, ConsistencyLevel.SERIAL));
                }

                Assert.assertTrue(proposalDeps.isEmpty());

                for (UpgradeNode node: unodes)
                {
                    node.upgradeService.reportPaxosCommit(commit, leader, ConsistencyLevel.SERIAL, proposalDeps);
                }

                expectedOrder.add(commit.ballot);

                proposalDeps.clear();
                commit = Commit.newPrepare(KEY, cfm, UUIDGen.getTimeUUID());
                for (UpgradeNode node: unodes.subList(0, 2))
                {
                    proposalDeps.addAll(node.upgradeService.reportPaxosProposal(commit, leader, ConsistencyLevel.SERIAL));
                }
                unodes.get(0).upgradeService.reportPaxosCommit(commit, leader, ConsistencyLevel.SERIAL, proposalDeps);
                expectedOrder.add(commit.ballot);
            }
        };

        // make sure the last node doesn't get the complete message
        unode.upgradeService.preCompleteHook = new Runnable()
        {
            @Override
            public void run()
            {
                unodes.get(unodes.size() - 1).setState(Node.State.DOWN);
            }
        };

        unode.upgradeService.upgradeNode();

        for (UpgradeNode node: unodes.subList(0, unodes.size() - 1))
        {
            Assert.assertTrue(node.upgradeService.isUpgraded());
            Assert.assertTrue(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }

        Assert.assertFalse(unodes.get(unodes.size() - 1).upgradeService.isUpgraded());
        Assert.assertFalse(unodes.get(unodes.size() - 1).upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));

        // now bring the last node up, and run an epaxos instance. Executions should be the same across nodes
        unodes.get(unodes.size() - 1).setState(Node.State.UP);

        UpgradeNode upgradeNode = unodes.get(unodes.size() - 1);
        upgradeNode.query(getSerializedCQLRequest(KEY_VAL, 0));
        Instance lastInstance = upgradeNode.getLastCreatedInstance();
        expectedOrder.add(lastInstance.getId());

        Assert.assertEquals(expectedOrder, upgradeNode.recordedExecutions);
        Assert.assertTrue(upgradeNode.upgradeService.isUpgraded());
        Assert.assertTrue(upgradeNode.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));

        // check that the epaxos service didn't re-execute paxos commits
        int numExec = expectedOrder.size();
        Assert.assertEquals(expectedOrder.subList(2, numExec), unodes.get(0).executionOrder);
        Assert.assertEquals(expectedOrder.subList(1, numExec), unodes.get(1).executionOrder);
        Assert.assertEquals(expectedOrder.subList(1, numExec), unodes.get(2).executionOrder);
    }

    @Test
    public void minorityReceivesCompleteMessage() throws Exception
    {
        for (UpgradeNode node: unodes)
        {
            Assert.assertFalse(node.upgradeService.isUpgraded());
            Assert.assertFalse(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
        }

        final List<UUID> expectedOrder = new ArrayList<>();
        final UpgradeNode unode = unodes.get(0);

        // only the first node gets the message, the other 2 commit one paxos round, and propose another
        unode.upgradeService.preCompleteHook = new Runnable()
        {
            @Override
            public void run()
            {
                // partition the completing node from the others
                unode.setNetworkZone(1);

                // do some commits on the others
                Commit commit;
                commit = Commit.newPrepare(KEY, cfm, UUIDGen.getTimeUUID());
                InetAddress leader = unodes.get(0).getEndpoint();
                Set<UUID> proposalDeps = new HashSet<>();
                for (UpgradeNode node: unodes.subList(1, 3))
                {
                    proposalDeps.addAll(node.upgradeService.reportPaxosProposal(commit, leader, ConsistencyLevel.SERIAL));
                }

                Assert.assertTrue(proposalDeps.isEmpty());

                for (UpgradeNode node: unodes.subList(1, 3))
                {
                    node.upgradeService.reportPaxosCommit(commit, leader, ConsistencyLevel.SERIAL, proposalDeps);
                }

                expectedOrder.add(commit.ballot);

                proposalDeps.clear();
                commit = Commit.newPrepare(KEY, cfm, UUIDGen.getTimeUUID());
                leader = unodes.get(2).getEndpoint();
                for (UpgradeNode node: unodes.subList(0, 2))
                {
                    proposalDeps.addAll(node.upgradeService.reportPaxosProposal(commit, leader, ConsistencyLevel.SERIAL));
                }
                unodes.get(2).upgradeService.reportPaxosCommit(commit, leader, ConsistencyLevel.SERIAL, proposalDeps);
                expectedOrder.add(commit.ballot);
            }
        };

        unode.upgradeService.upgradeNode();
        Assert.assertTrue(unode.upgradeService.isUpgraded());
        Assert.assertTrue(unode.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));

        for (UpgradeNode node: unodes.subList(1, 3))
        {
            Assert.assertFalse(node.upgradeService.isUpgraded());
            Assert.assertFalse(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
            Assert.assertEquals(TokenState.State.UPGRADING, node.getTokenStateManager(Scope.GLOBAL).get(TOKEN, CFID).getState());
        }

        // add the other node to the partition (so node0 can talk with node1)
        unodes.get(1).setNetworkZone(1);
        unode.query(getSerializedCQLRequest(KEY_VAL, 0));
        expectedOrder.add(unode.getLastCreatedInstance().getId());

        // check that the first 2 nodes are upgraded and have the same execution order
        for (UpgradeNode node: unodes.subList(0, 2))
        {
            Assert.assertTrue(node.upgradeService.isUpgraded());
            Assert.assertTrue(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
            Assert.assertEquals(node.recordedExecutions, expectedOrder);
        }

        // now bring the 3rd node back
        unodes.get(2).setNetworkZone(1);
        unode.query(getSerializedCQLRequest(KEY_VAL, 0));
        expectedOrder.add(unode.getLastCreatedInstance().getId());

        // check all nodes are on the same page
        for (UpgradeNode node: unodes)
        {
            Assert.assertTrue(node.upgradeService.isUpgraded());
            Assert.assertTrue(node.upgradeService.isUpgradedForQuery(TOKEN, CFID, Scope.GLOBAL));
            Assert.assertEquals(node.recordedExecutions, expectedOrder);
        }

        // check that the epaxos service didn't re-execute paxos commits
        int numExec = expectedOrder.size();
        Assert.assertEquals(expectedOrder, unodes.get(0).executionOrder);
        Assert.assertEquals(expectedOrder.subList(1, numExec), unodes.get(1).executionOrder);
        Assert.assertEquals(expectedOrder.subList(2, numExec), unodes.get(2).executionOrder);
    }
}
