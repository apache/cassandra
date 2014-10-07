package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;

public class EpaxosTokenIntegrationTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    private static BytesToken tokenFor(int i)
    {
        return tokenFor(ByteBufferUtil.bytes(i));
    }

    private static BytesToken tokenFor(ByteBuffer k)
    {
        return new BytesToken(k);
    }

    private static final Token TOKEN1 = tokenFor(100);
    private static final Token TOKEN2 = tokenFor(200);

    static class IntegrationTokenStateManager extends TokenStateManager
    {
        IntegrationTokenStateManager(String keyspace, String table, Scope scope)
        {
            super(keyspace, table, scope);
            start();
        }

        private volatile Set<Range<Token>> replicatedRanges = Collections.emptySet();


        public void setReplicatedRanges(Set<Range<Token>> replicatedRanges)
        {
            this.replicatedRanges = replicatedRanges;
        }

        @Override
        protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
        {
            return replicatedRanges;
        }
    }

    private static class Service extends Node.SingleThreaded
    {
        Service(int number, Messenger messenger, String dc, String ksName)
        {
            super(number, messenger, dc, ksName);
        }

        @Override
        protected void scheduleTokenStateMaintenanceTask()
        {
            // no-op
        }

        @Override
        protected TokenStateManager createTokenStateManager(Scope scope)
        {
            return new IntegrationTokenStateManager(getKeyspace(), getTokenStateTable(), scope);
        }
    }

    @Override
    public Node createNode(int nodeNumber, Messenger messenger, String dc, String ks)
    {
        return new Service(nodeNumber, messenger, dc, ks);
    }

    @Test
    public void successCase()
    {
        // check baseline
        for (Node node: nodes)
        {
            IntegrationTokenStateManager tsm = (IntegrationTokenStateManager) node.getTokenStateManager(DEFAULT_SCOPE);
            tsm.setReplicatedRanges(Sets.newHashSet(range(TOKEN0, TOKEN2)));

            // token states for the currently replicated tokens should be implicitly initialized
            tsm.getOrInitManagedCf(CFID);
            Assert.assertEquals(Lists.newArrayList(TOKEN2), node.getTokenStateManager(DEFAULT_SCOPE).getManagedTokensForCf(CFID));
            Assert.assertEquals(0, node.getCurrentEpoch(TOKEN2, CFID, DEFAULT_SCOPE));

            // add some key states
            KeyStateManager ksm = node.getKeyStateManager(DEFAULT_SCOPE);
            for (int i=0; i<4; i++)
            {
                ByteBuffer key = ByteBufferUtil.bytes(i);
                ksm.loadKeyState(key, CFID);
            }
        }

        nodes.get(0).addToken(CFID, TOKEN1, DEFAULT_SCOPE);

        // check new token exists, and epochs have been incremented
        for (Node node: nodes)
        {
            List<Token> expectedTokens = Lists.newArrayList(TOKEN1, TOKEN2);
            List<Token> actualTokens = node.getTokenStateManager(DEFAULT_SCOPE).allTokenStatesForCf(CFID);
            Assert.assertEquals(expectedTokens, actualTokens);

            TokenState ts1 = node.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN1, CFID);
            Assert.assertNotNull(ts1);
            Assert.assertEquals(1, ts1.getEpoch());

            TokenState ts2 = node.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN2, CFID);
            Assert.assertNotNull(ts2);
            Assert.assertEquals(1, ts2.getEpoch());
        }

        // check that duplicate token inserts are ignored
        nodes.get(1).addToken(CFID, TOKEN1, DEFAULT_SCOPE);

        for (Node node: nodes)
        {
            List<Token> expectedTokens = Lists.newArrayList(TOKEN1, TOKEN2);
            List<Token> actualTokens = node.getTokenStateManager(DEFAULT_SCOPE).allTokenStatesForCf(CFID);
            Assert.assertEquals(expectedTokens, actualTokens);

            TokenState ts1 = node.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN1, CFID);
            Assert.assertNotNull(ts1);
            Assert.assertEquals(1, ts1.getEpoch());

            TokenState ts2 = node.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN2, CFID);
            Assert.assertNotNull(ts2);
            Assert.assertEquals(1, ts2.getEpoch());
        }
    }

    @Test
    public void executionSuccessCase() throws Exception
    {
        Node node = nodes.get(0);

        IntegrationTokenStateManager tsm = (IntegrationTokenStateManager) node.getTokenStateManager(DEFAULT_SCOPE);
        tsm.setReplicatedRanges(Sets.newHashSet(range(TOKEN0, TOKEN2)));
        tsm.getOrInitManagedCf(CFID);

        TokenState ts = tsm.getExact(TOKEN2, CFID);
        Assert.assertNotNull(ts);

        Map<Token, UUID> fakeIds = Maps.newHashMap();
        Map<Token, ByteBuffer> fakeKeys = Maps.newHashMap();

        // add some key states and pending token instances
        // this will create a key state for tokens 50, 100, 150, & 200, as well as token state
        // dependencies at 50 & 150
        KeyStateManager ksm = node.getKeyStateManager(DEFAULT_SCOPE);
        for (int i=0; i<4; i++)
        {
            int iKey = (i * 50) + 50;
            ByteBuffer key = ByteBufferUtil.bytes(iKey);
            Token token = tokenFor(iKey);
            ksm.loadKeyState(key, CFID);
            fakeKeys.put(token, key);
            if (i%2 == 0)
            {
                UUID id = UUIDGen.getTimeUUID();
                fakeIds.put(token, id);
                ts.recordTokenInstance(token, id);
            }
        }

        TokenInstance instance = new TokenInstance(node.getEndpoint(), CFID, TOKEN1, ts.getRange(), DEFAULT_SCOPE);
        node.getCurrentDependencies(instance);
        instance.setDependencies(Collections.<UUID>emptySet());
        instance.setState(Instance.State.COMMITTED);
        node.saveInstance(instance);
        Assert.assertTrue(ts.getCurrentTokenInstances(new Range<>(TOKEN2, TOKEN1)).contains(instance.getId()));

        new ExecuteTask(node, instance.getId()).run();

        TokenState ts2 = tsm.getExact(TOKEN1, CFID);
        Assert.assertNotNull(ts2);

        // check instance is marked executed
        Assert.assertEquals(Instance.State.EXECUTED, instance.getState());
        Assert.assertEquals(TokenState.State.NORMAL, ts2.getState());

        // check first token state
        Assert.assertEquals(1, ts.getEpoch());
        Set<UUID> deps = ts.getCurrentTokenInstances(new Range<>(TOKEN1, TOKEN2));
        Assert.assertEquals(1, deps.size());
        Assert.assertEquals(fakeIds.get(tokenFor(150)), deps.iterator().next());

        // check second token state
        Assert.assertEquals(1, ts2.getEpoch());
        deps = ts2.getCurrentTokenInstances(new Range<>(TOKEN2, TOKEN1));
        Assert.assertEquals(1, deps.size());
        Assert.assertEquals(fakeIds.get(tokenFor(50)), deps.iterator().next());
        Assert.assertEquals(Sets.newHashSet(instance.getId()), ts2.getCurrentEpochInstances());

        Map<Token, Long> expectedEpochs = Maps.newHashMap();
        expectedEpochs.put(TOKEN1, 1l);
        expectedEpochs.put(TOKEN2, 1l);
        // check key states
        for (Map.Entry<Token, ByteBuffer> entry: fakeKeys.entrySet())
        {
            ByteBuffer key = entry.getValue();

            KeyState ks = ksm.loadKeyState(key, CFID);
            Assert.assertEquals(1l, ks.getEpoch());
        }

        // increment the epoch for just the first token state
        EpochInstance epochInstance = node.createEpochInstance(TOKEN2, CFID, 2, DEFAULT_SCOPE);
        epochInstance.setDependencies(node.getCurrentDependencies(epochInstance).left);
        epochInstance.setState(Instance.State.COMMITTED);
        node.saveInstance(epochInstance);
        node.executeEpochInstance(epochInstance);

        Assert.assertEquals(2, ts.getEpoch());
        Assert.assertEquals(1, ts2.getEpoch());

        // keys with tokens TOKEN1 < t <= TOKEN2 should be at epoch 2
        for (Map.Entry<Token, ByteBuffer> entry: fakeKeys.entrySet())
        {
            Token token = entry.getKey();
            ByteBuffer key = entry.getValue();

            KeyState ks = ksm.loadKeyState(key, CFID);
            long expectedEpoch = token.compareTo(TOKEN1) >= 1 ? 2 : 1;
            Assert.assertEquals(String.format("Token: " + token.toString()), expectedEpoch, ks.getEpoch());
        }
    }

    /**
     * Tests that, given a range (0, 300] and existing keys at 50, 150, & 250 , running concurrent instances
     * for (0, 100] and (0, 200] will end up with the ranges (0, 100], (100, 200], and (200, 300], all of the
     * dependencies for both instances are acknowledged, and the token state manager reports these ranges
     * as replicated
     */
    @Test
    public void overlappingInstances() throws Exception
    {
        final Token token100 = token(100);
        final Token token200 = token(200);
        Token token300 = token(300);

        for (Node node: nodes)
        {
            IntegrationTokenStateManager tsm = (IntegrationTokenStateManager) node.getTokenStateManager(DEFAULT_SCOPE);
            tsm.setReplicatedRanges(Sets.newHashSet(range(TOKEN0, token300)));
            tsm.getOrInitManagedCf(cfm.cfId);

            TokenState ts = tsm.getExact(token300, cfm.cfId);
            Assert.assertNotNull(ts);
        }

        Iterator<Node> iterator = nodes.iterator();

        final Node node1 = iterator.next();
        final Node node2 = iterator.next();
        final Node node3 = iterator.next();

        node1.query(newSerializedRequest(getCqlCasRequest(50, 0, ConsistencyLevel.SERIAL), key(50)));
        Instance instance050 = node1.getLastCreatedInstance();
        printInstance(instance050, "instance050");
        Assert.assertEquals(Instance.State.EXECUTED, instance050.getState());
        Assert.assertEquals(Collections.<UUID>emptySet(), instance050.getDependencies());

        node2.query(newSerializedRequest(getCqlCasRequest(150, 0, ConsistencyLevel.SERIAL), key(150)));
        Instance instance150 = node2.getLastCreatedInstance();
        printInstance(instance150, "instance150");
        Assert.assertEquals(Instance.State.EXECUTED, instance150.getState());
        Assert.assertEquals(Collections.<UUID>emptySet(), instance150.getDependencies());

        node3.query(newSerializedRequest(getCqlCasRequest(250, 0, ConsistencyLevel.SERIAL), key(250)));
        Instance instance250 = node3.getLastCreatedInstance();
        printInstance(instance250, "instance250");
        Assert.assertEquals(Instance.State.EXECUTED, instance250.getState());
        Assert.assertEquals(Collections.<UUID>emptySet(), instance250.getDependencies());

        Node.queuedExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                // setting node2 down will force an accept because node 1 & 3
                // are guaranteed to have different deps
                node2.setState(Node.State.DOWN);
                node1.addToken(cfm.cfId, token100, DEFAULT_SCOPE);
                node3.addToken(cfm.cfId, token200, DEFAULT_SCOPE);
            }
        });

        Set<UUID> globalDeps = Sets.newHashSet(instance050.getId(), instance150.getId(), instance250.getId());

        TokenInstance tokenInstance100 = (TokenInstance) node1.getLastCreatedInstance();
        printInstance(tokenInstance100, "tokenInstance100");
        TokenInstance tokenInstance200 = (TokenInstance) node3.getLastCreatedInstance();
        printInstance(tokenInstance200, "tokenInstance200");

        Assert.assertEquals(Sets.union(globalDeps, Sets.newHashSet(tokenInstance200.getId())), tokenInstance100.getDependencies());
        Assert.assertEquals(Sets.union(globalDeps, Sets.newHashSet(tokenInstance100.getId())), tokenInstance200.getDependencies());

        // run a new set of instances for the previous key states, they should only point to the token instances
        Set<UUID> expectedDeps = Sets.newHashSet(tokenInstance200.getId());

        // not bringing node2 back up because it's out of date, and will introduce the original key(50) instance
        // as a dependency. The goal of this test, however, is to check the behavior of the key state manager when
        // there are multiple token instances (with different tokens) in flight for the same token range

        node1.query(newSerializedRequest(getCqlCasRequest(50, 0, ConsistencyLevel.SERIAL), key(50)));
        instance050 = node1.getLastCreatedInstance();
        printInstance(instance050, "instance050");
        Assert.assertEquals(Instance.State.EXECUTED, instance050.getState());
        Assert.assertEquals(expectedDeps, instance050.getDependencies());

        node1.query(newSerializedRequest(getCqlCasRequest(150, 0, ConsistencyLevel.SERIAL), key(150)));
        instance150 = node1.getLastCreatedInstance();
        printInstance(instance150, "instance150");
        Assert.assertEquals(Instance.State.EXECUTED, instance150.getState());
        Assert.assertEquals(expectedDeps, instance150.getDependencies());

        node1.query(newSerializedRequest(getCqlCasRequest(250, 0, ConsistencyLevel.SERIAL), key(250)));
        instance250 = node1.getLastCreatedInstance();
        printInstance(instance250, "instance250");
        Assert.assertEquals(Instance.State.EXECUTED, instance250.getState());
        Assert.assertEquals(expectedDeps, instance250.getDependencies());
    }
}
